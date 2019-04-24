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
	* 该DStream不使用Receiver，而是直接创建KafkaRDD从指定偏移量范围
	* 读取记录，不会提交偏移量到ZK上。已消费的偏移量有该DStream自己负责
	* 跟踪记录，如果需要更新ZK/Kafka上的偏移量，需要用户在应用程序中手动
	* 进行更新，用户可以在generatedRDD中获取每一个BatchDuration产生的
	* 偏移量范围。
	*
	* 1、、DirectKafkaInputDStream有一些列KafkaRDD构成，每个RDD内部的每
	* 一个分区对应一个Kafka的主题分区。
	*
	* 2、KafkaRDD + RateController实现一系列RDD的创建。
	* spark.streaming.kafka.maxRatePerPartition用于控制每个分区读
	* 取消息的速度，单位为s。
	*
	* [Exactly Once语义]
	* 1、transformations exactly once
	*
	* KafkaReceiver/ReliableKafkaReceiver的问题：重复消费
	* 由于KafkaRDD是根据Receiver从Kafka主题拉取的数据在每个Batch Interval
	* 产生的，而消费的记录的偏移量是在Receiver内部自己维护或采用Kafka
	* 的auto.commit.enable维护(默认每5s提交一次)，两者在出现故障时都
	* 会产生重复消费问题。下面以ReliableKafkaReceiver为例进行说明：
	*
	* --------------------------------------------------->
	*     batch interval
	* ----------@@----------@@----------@@--------------->
	*          RDD1        RDD2         X
	*                                  /|\发生故障
	* 如上图所示，每个batch interval产生一个Block（后续基于其创建
	* KafkaRDD），每次产生KafkaRDD都会更新ZK/Kafka中偏移量，在第三次
	* 产生KafkaRDD成功但还没有来得及提交偏移量时程序崩溃，因此已经产生
	* KafkaRDD当偏移量没有被提交。在应用程序恢复重启后，上一次未来的及
	* 提交会被继续消费并添加到创建的新的KafkaRDD中，这样在后续流应用程
	* 序处理数据时就会出现重复消费————同一条记录会被处理多次。
	*
	* 采用DirectKafkaInputDStream为什么能保证一条记录只会被处理一次！
	* 因为没有使用Receiver，每次Batch Interval生成的KafkaRDD是根据
	* 分区最大处理记录数和每个Batch流入记录的最小值确定当前KafkaRDD能
	* 够处理的偏移量范围，并且每次被消费的偏移量范围都会通过WAL进行保存。
	* 当应用程序崩溃时，根据WAL checkpoint进行恢复，可以得到上一次创建
	* KafkaRDD时偏移量的位置，接下来产生的KafkaRDD就会在这个记录的偏
	* 移量的基础上继续在每个Batch Interval产生KafkaRDD。这样可以保证
	* 所有产生的KafkaRDD中没有重复的Kafka记录，因此就不会出现重复消费
	* 问题。
	*
	* 因此DirectKafkaInputDStream可以保证每条记录被接收并且处理一次，
	* 但是不能保证被处理后的数据仅且会被输出一次！
	*
	* 2、outputted exactly once
	*
	* 3、End-to-end exactly-once semantics需要满足下面两个条件
	*    + transformations exactly once
	*    + outputted exactly once
	*
	*   为了实现端到端的一次性意义，有两种方法：
	*  (1)、确保输出操作的幂等性(idempotent)，例如：存储系统的记录支持
	*    唯一主键，相同记录写入多次时忽略错误；
	*  (2)、外部存储系统支持事务操作，例如：MySQL通过事务确保操作原子性
	*  不允许部分成功部分失败。
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
