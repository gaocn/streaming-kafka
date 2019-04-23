package org.apache.spark.streaming.kafka.dstream

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.{DStreamCheckpointData, InputDStream}
import org.apache.spark.streaming.kafka.KafkaCluster
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.kafka.domain.OffsetRange
import org.apache.spark.streaming.kafka.rdd.KafkaRDD
import org.apache.spark.streaming.scheduler.{RateController, StreamInputInfo}
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.streaming.{StreamingContext, Time}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.ClassTag

/**
	* 该流"不提交偏移量"，直接创建KafkaRDD而不是通过Receiver接收
	* 数据并在每次batch interval时创建，从而实现语义一致性（Exactly
	* Once，消息仅且消费一次）。
	*
	* KafkaRDD + RateController实现一系列RDD的创建
	*
	* 该流有一些列[[org.apache.spark.streaming.kafka.rdd.KafkaRDD]]
	* 构成，每个RDD内部的每一个分区对应一个Kafka的主题分区。
	*
	* spark.streaming.kafka.maxRatePerPartition用于控制每个分区读
	* 取消息的速度，单位为s。
	*
	* 每次读取时需要指定起始位置，由于偏移量是由用户维护，因此在错误恢复
	* 或应用程序时，需要用户显示指定要读的位置。
	*
	* @param fromOffsets    准备读取数据的Kafka各分区的起始偏移量
	* @param messageHandler 读取消息后的处理句柄
	*/
class DirectKafkaInputDStream[
K: ClassTag,
V: ClassTag,
U <: Decoder[_] : ClassTag,
T <: Decoder[_] : ClassTag,
R: ClassTag
](
	 ssc: StreamingContext,
	 val kafkaParams: Map[String, String],
	 val fromOffsets: Map[TopicAndPartition, Long],
	 messageHandler: MessageAndMetadata[K, V] => R
 ) extends InputDStream[R](ssc) with Logging {

	/**
		*  ??? 要确保语义一致性，Spark应用程序读取kafka失败后就不能重试，因为
		* 偏移量是由用户维护，重试机制会导致用户无法准确维护偏移量。
		*/
	val maxRetries = context.sparkContext.getConf.getInt(
		"spark.streaming.kafka.maxRetries", 1)

	private[streaming] override def name: String = s"Kafka Direct Stream ${id}"

	protected[streaming] override val checkpointData = new DirectKafkaInputDStreamCheckpointData

	protected val kc = KafkaCluster(kafkaParams)

	/**
		* Receiver采用异步方式从ReceiverTracker中获取或维护数据接收速率
		*/
	override protected[streaming] val rateController: Option[RateController] = {
		if (RateController.isBackPressureEnabled(ssc.conf)) {
			Some(new DirectKafkaRateController(id, RateEstimator.create(ssc.conf, context.graph.batchDuration)))
		} else {
			None
		}
	}

	protected var currentOffset = fromOffsets

	//控制每个分区接收数据速率
	private val maxRateLimitPerPartition: Int = context.sparkContext.getConf
		.getInt("spark.streaming.kafka.maxRatePerPartition", 0)

	/** 每个BatchDuration每个分区能够接收的最大消息总量 */
	protected def maxMessagePerPartition:Option[Long] = {
		val estimatedRateLimit = rateController.map(_.getLatestRate().toInt)
		val numPartitioins = currentOffset.keys.size

		val effectiveRateLimitPerPartition =estimatedRateLimit.filter(_ > 0)
  		.map{limit =>
				if (maxRateLimitPerPartition > 0) {
					Math.min(maxRateLimitPerPartition, limit/numPartitioins)
				} else {
					limit / numPartitioins
				}
			}.getOrElse(maxRateLimitPerPartition)

		if (effectiveRateLimitPerPartition > 0) {
			val secondsPerBatch = context.graph.batchDuration.milliseconds.toDouble / 1000
			Some((secondsPerBatch * effectiveRateLimitPerPartition).toLong)
		} else {
			None
		}
	}

	@tailrec
	protected final def latestLeaderOffset(retries: Int): Map[TopicAndPartition, LeaderOffset] = {
		val o = kc.getLatestLeadereOffset(currentOffset.keySet)
		if (o.isLeft) {
			val err =  o.left.get.toString
			if (retries <= 0) {
				throw new Exception(err)
			} else {
				logError(err)
				Thread.sleep(kc.config.refreshLeaderBackoffMs)
				latestLeaderOffset(retries - 1)
			}
		} else {
			o.right.get
		}
	}

	/**
		* 限制每个分区中最大消息条数
		*/
	protected def clamp(leaderOffsets: Map[TopicAndPartition, LeaderOffset]):Map[TopicAndPartition, LeaderOffset] = {
		maxMessagePerPartition.map{mmp =>
			leaderOffsets.map{case(tp, lo)=>
				tp -> lo.copy(offset = Math.min(currentOffset(tp) + mmp, lo.offset))
			}
		}.getOrElse(leaderOffsets)
	}

	override def compute(validTime: Time):Option[KafkaRDD[K,V,U,T,R]] = {
				val untilOffsets = clamp(latestLeaderOffset(maxRetries))

			val rdd = KafkaRDD[K,V,U,T,R](context.sparkContext, kafkaParams,currentOffset, untilOffsets, messageHandler)


		/**
			* 向InputInfoTracker报告当前Batch的记录数目和元数据
			*/
		val offsetRanges = currentOffset.map{case(tp, offset) =>
			val uo = untilOffsets(tp)
			OffsetRange(tp.topic, tp.partition, offset, uo.offset)
		}

		val description = offsetRanges.filter{offsetRange =>
			offsetRange.fromOffset != offsetRange.untilOffset
		}.map{offsetRange=>
			s"topic: ${offsetRange.topic} \t partition: ${offsetRange.partition}\t" +
			s"offsets: ${offsetRange.fromOffset} to ${offsetRange.untilOffset}"
		}.mkString("\n")

		val metadata = Map(
			"offsets"  -> offsetRanges.toList,
			StreamInputInfo.METADATA_KEY_DESCRIPTION -> description
		)
		val inputInfo = StreamInputInfo(id, rdd.count, metadata)
		ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

		currentOffset = untilOffsets.map(kv => kv._1 -> kv._2.offset)
		Some(rdd)
	}

	override def start(): Unit = {}

	override def stop(): Unit = {}

	private[streaming] class DirectKafkaInputDStreamCheckpointData extends DStreamCheckpointData(this) {
		//(String, Int, Long, Long) -> (topic, partition, fromOffset, untilOffset)
		def batchForTime: mutable.HashMap[Time, Array[(String, Int, Long, Long)]] = {
			data.asInstanceOf[mutable.HashMap[Time, Array[OffsetRange.OffsetRangeTuple]]]
		}

		override def update(time: Time): Unit = {
			batchForTime.clear()
			generatedRDDs.foreach { kv =>
				val a = kv._2.asInstanceOf[KafkaRDD[K, V, U, T, R]].offsetRanges.map(_.toTuple)
				batchForTime += kv._1 -> a
			}
		}

		override def cleanup(time: Time): Unit = {}

		override def restore(): Unit = {
			val topics = fromOffsets.keySet
			val leaders = KafkaCluster.checkErrors(kc.findLeaders(topics))

			batchForTime.toSeq.sortBy(_._1)(Time.ordering).foreach { case (t, b) =>
				logInfo(s"恢复${t}时刻KafkaRDD的数据：${b.mkString("[ ", ", ", " ]")}")
				generatedRDDs += t -> new KafkaRDD[K, V, U, T, R](context.sparkContext, kafkaParams, b.map(OffsetRange(_)), leaders, messageHandler)
			}
		}
	}

	/**
		* 从RateEstimator从获取当前数据速率
		*/
	private[streaming] class DirectKafkaRateController(id: Int, estimator: RateEstimator) extends RateController(id, estimator) {
		override protected def publish(rate: Long): Unit = {
			logInfo(s"当前预测速率为：rate=${rate}")
		}
	}

}
