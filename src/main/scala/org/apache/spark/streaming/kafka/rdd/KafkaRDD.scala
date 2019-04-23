package org.apache.spark.streaming.kafka.rdd

import kafka.api.{FetchRequest, FetchRequestBuilder, FetchResponse}
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.message.{MessageAndMetadata, MessageAndOffset}
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import org.apache.spark._
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaCluster
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.kafka.domain.{HasOffsetRanges, OffsetRange}
import org.apache.spark.util.NextIterator

import scala.collection.mutable.ArrayBuffer
import scala.reflect.{ClassTag, classTag}

/**
	* 批量消费Kafka数据的，指定消费批次的起始偏移量以便实现Exactly Once语义
	* @param kafkaParam 消费者连接kafka broker的参数
	* @param offsetRanges 当前RDD中对应的Kafka中的数据的各个偏移量范围
	* @param leaders 各个分区首领所在的kafka brokers的地址
	* @param messageHandler 用于将Kafka Record转换层指定类型的数据
	* @tparam K Kafka记录的键类型
	* @tparam V Kafka记录的值类型
	* @tparam U Kafka记录的键解码器
	* @tparam T Kafka记录的值的解码器
	* @tparam R 转换kafka记录后的追踪数据元素类型
	*/
class KafkaRDD[
K: ClassTag,
V: ClassTag,
U <: Decoder[_] : ClassTag,
T <: Decoder[_] : ClassTag,
R: ClassTag] private[spark](
		sc: SparkContext,
		kafkaParam: Map[String, String],
		val offsetRanges: Array[OffsetRange],
		leaders: Map[TopicAndPartition, (String, Int)],
		messageHandler: MessageAndMetadata[K,V] => R
) extends RDD[R](sc, Nil) with Logging with HasOffsetRanges {

	/**
		* 有多少个[[OffsetRange]]有对应多少个RDD分区
		*/
	override protected def getPartitions: Array[Partition] = {
		offsetRanges.zipWithIndex.map{case (offsetRange, index) =>
			val (host, port) = leaders(new TopicAndPartition(offsetRange.topic, offsetRange.partition))
			new KafkaRDDPartition(index, offsetRange.topic, offsetRange.partition, offsetRange.fromOffset, offsetRange.untilOffset, host, port)
		}.toArray
	}

	override def count(): Long = offsetRanges.map(_.count()).sum
	override def isEmpty(): Boolean = count ==  0L

	override def countApprox(timeout: Long, confidence: Double  =  0.95): PartialResult[BoundedDouble] = {
		val c  = count
		new PartialResult(new BoundedDouble(c, 1.0, c, c), true)
	}

	override def take(num: Int): Array[R] = {
		val nonEmptyPartition = this.partitions.map(_.asInstanceOf[KafkaRDDPartition]).filter(_.count() > 0)

		if (num < 1 || nonEmptyPartition.size < 1) {
			return new Array[R](0)
		}

		/**
			* 计算从每个分区取出多少数据
			*/
		val parts = nonEmptyPartition.foldLeft(Map[Int, Int]()){(result, part)=>
			val remain = num - result.values.sum
			if(remain > 0) {
				val taken = (Math.min(remain, part.count)).toInt
				result + (part.index -> taken)
			} else {
				result
			}
		}

		val buf = new ArrayBuffer[R]()
		val res = context.runJob(this,
			(tc: TaskContext, iter: Iterator[R]) => iter.take(parts(tc.partitionId())).toArray,
			parts.keys.toArray
		)
		res.foreach(buf ++= _)
		buf.toArray
	}

	override def getPreferredLocations(split: Partition): Seq[String] = {
		val partition = split.asInstanceOf[KafkaRDDPartition]
		Seq(partition.host)
	}

	private def errBeginAfterEnd(part: KafkaRDDPartition): String =
		s"Beginning offset ${part.fromOffset} is After the ending offset ${part.untilOffset} " +
			s"for topic ${part.topic} partition ${part.partition}. " +
			s"You either provided an invalid fromOffset, or the kafka topic has been damaged."

	private def errRanOutBeforeEnd(part: KafkaRDDPartition):String =
		s"Ran out of messages before reaching ending offset ${part.untilOffset} " +
			s"for topic ${part.topic} partition ${part.partition} start ${part.fromOffset}." +
			" This should not happen, and indicates that messages may have been lost"

	private def  errOvershotEnd(itemOffset: Long, part: KafkaRDDPartition):String =
		s"Got ${itemOffset} > ending offset ${part.untilOffset} " +
			s"for topic ${part.topic} partition ${part.partition} start ${part.fromOffset}." +
			" This should not happen, and indicates a message may have been skipped"

	override def compute(split: Partition, context: TaskContext): Iterator[R] = {
		val partition = split.asInstanceOf[KafkaRDDPartition]
		assert(partition.fromOffset <= partition.untilOffset, errBeginAfterEnd(partition))

		if (partition.fromOffset == partition.untilOffset) {
			log.info(s"Beginning offset ${partition.fromOffset} is the same as ending offset " +
				s"skipping ${partition.topic} ${partition.partition}")
			Iterator.empty
		} else {
			new KafkaRDDIterator(partition, context)
		}
	}

	/**
		* 从Kafka的特定分区获取数据
		*
		* @param part RDD分区
		* @param context TaskContext
		* @tparam R 数据类型
		*/
	class KafkaRDDIterator(part: KafkaRDDPartition, context: TaskContext) extends NextIterator[R] with Logging{
		context.addTaskCompletionListener(context => closeIfNeeded())
		log.info(s"Computing topic ${part.topic},partition ${part.partition} " +
			s"offset ${part.fromOffset} -> ${part.untilOffset}")

		val kc = new KafkaCluster(kafkaParam)
		val keyDecoder = classTag[U].runtimeClass.getConstructor(classOf[VerifiableProperties])
  		.newInstance(kc.config.props).asInstanceOf[Decoder[K]]
		val valueDecoder = classTag[T].runtimeClass.getConstructor(classOf[VerifiableProperties])
  		.newInstance(kc.config.props).asInstanceOf[Decoder[V]]
		var consumer:SimpleConsumer = null
		var requestOffset = part.fromOffset
		var iter: Iterator[MessageAndOffset] = null

		private def connectLeader():SimpleConsumer= {
			if (context.attemptNumber() > 0) {
				kc.connectLeader(part.topic, part.partition).fold(errs => throw new Exception(
					s"Couldn't connect to leader for topic ${part.topic} ${part.partition}: " +
						errs.mkString("\n")) ,
					consumer=>consumer)
			} else {
				kc.connect(part.host, part.port)
			}
		}

		private def fetch(req: FetchRequest): FetchResponse = {
			if (consumer == null) {
				consumer = connectLeader
				log.info(s"连接到broker：(${consumer.host},${consumer.port})")
			}
			val response = consumer.fetch(req)
			return response
		}

		private def handleFetchErr(res: FetchResponse): Unit = {
			if (res.hasError) {
				val err = res.errorCode(part.topic, part.partition)
				if (err == ErrorMapping.LeaderNotAvailableCode ||
				err == ErrorMapping.NotLeaderForPartitionCode) {
					log.error(s"Lost Leader for topic ${part.topic} partition ${part.partition}," +
						s"sleeping for ${kc.config.refreshLeaderBackoffMs}ms")
					Thread.sleep(kc.config.refreshLeaderBackoffMs)
				}

				/**
					* 抛出异常给RDD，让RDD进行重试
					*/
				throw ErrorMapping.exceptionFor(err)
			}
		}

		private def fetchBatch:Iterator[MessageAndOffset] = {
			val req = new FetchRequestBuilder()
  			.addFetch(part.topic, part.partition, part.fromOffset, kc.config.fetchMessageMaxBytes)
  			.build()

			val resp = fetch(req)
			handleFetchErr(resp)

			//若成功获取数据
			resp.messageSet(part.topic, part.partition)
  			.iterator
  			.dropWhile(_.offset < requestOffset)
		}

		override protected def getNext(): R = {
			if (iter == null || !iter.hasNext) {
				iter = fetchBatch
			}

			if (!iter.hasNext) {
				assert(requestOffset == part.untilOffset, errRanOutBeforeEnd(part))
				finished = true
				null.asInstanceOf[R]
			} else {
				val item = iter.next();
				if(item.offset >= part.untilOffset) {
					assert(requestOffset == part.untilOffset, errOvershotEnd(item.offset,part))
					finished = true
					null.asInstanceOf[R]
				} else {
					requestOffset = item.nextOffset
					messageHandler(new MessageAndMetadata(part.topic, part.partition,item.message, item.offset,keyDecoder, valueDecoder))
				}
			}
		}

		override protected def close(): Unit = {
			if (consumer != null) {
				consumer.close()
			}
		}
	}
}

private[kafka] object KafkaRDD {
	def apply[
	K: ClassTag,
	V: ClassTag,
	U <: Decoder[_] : ClassTag,
	T <: Decoder[_] : ClassTag,
	R: ClassTag](
								sc: SparkContext,
								kafkaParam: Map[String, String],
								fromOffsets: Map[TopicAndPartition, Long],
								untilOffsets: Map[TopicAndPartition, LeaderOffset],
								messageHandler: MessageAndMetadata[K, V] => R
	): KafkaRDD[K,V,U,T,R] = {
		val leaders = untilOffsets.map{case(tp, lo) =>
			tp -> (lo.host, lo.port)
		}

		val offsetRanges = fromOffsets.map{case(tp, offset) =>
				val untilOffset = untilOffsets(tp)
				OffsetRange(tp.topic,tp.partition, offset,untilOffset.offset)
		}.toArray

		new KafkaRDD[K,V,U,T,R](sc, kafkaParam, offsetRanges, leaders, messageHandler)
	}
}