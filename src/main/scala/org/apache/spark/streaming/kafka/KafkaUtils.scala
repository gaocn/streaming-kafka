package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{Decoder, StringDecoder}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.domain.OffsetRange
import org.apache.spark.streaming.kafka.dstream.{DirectKafkaInputDStream, KafkaInputDStream}
import org.apache.spark.streaming.kafka.rdd.KafkaRDD
import org.apache.spark.streaming.util.WriteAheadLogUtils

import scala.reflect.ClassTag

object KafkaUtils {


	/**
		* 创建一个从Kafka Broker 拉取消息的输入流
		* @param ssc Streaming Context
		* @param kafkaParams kafka相关参数
		* @param topics (topic_name -> numPartitions)，每个分区用
		*               单独的消费者消费额
		* @param storageLevel 存储拉取数据的级别
		* @tparam K
		* @tparam V
		* @tparam U
		* @tparam T
		* @return DStream of (Kafka message key, Kafka message value)
		*/
	def createDStream[K: ClassTag, V: ClassTag, U <: Decoder[_] : ClassTag, T <: Decoder[_] : ClassTag](
		ssc: StreamingContext,
		kafkaParams: Map[String, String],
		topics: Map[String, Int],
		storageLevel: StorageLevel
	): ReceiverInputDStream[(K, V)] = {
		val walEnabled = WriteAheadLogUtils.enableReceiverLog(ssc.conf)
		new KafkaInputDStream[K,V,U,T](ssc, kafkaParams, topics, walEnabled,storageLevel)
	}

	def createDStream(
		  ssc: StreamingContext,
			zkQuorum: String,
			groupId: String,
			topics: Map[String, Int],
			storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
		 ): ReceiverInputDStream[(String, String)]  = {
		val kafkaParams = Map[String, String](
			"zookeeper.connect" -> zkQuorum,
			"group.id" -> groupId,
			"zookeeper.connection.timeour.ms" -> "10000"
		)
		createDStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams,topics, storageLevel)
	}

	/** get leaders for the given offset ranges, or throw an exception */
	private def leadersForRanges(kc: KafkaCluster,offsetRanges: Array[OffsetRange]): Map[TopicAndPartition, (String, Int)] = {
		val topics = offsetRanges.map(o => TopicAndPartition(o.topic, o.partition)).toSet
		val leaders = kc.findLeaders(topics)
		KafkaCluster.checkErrors(leaders)
	}

	/**
		* 根据指定偏移量范围获取数据并创建RDD
		* @param sc
		* @param kafkaParams
		* @param offsetRanges
		* @tparam K
		* @tparam V
		* @tparam KD
		* @tparam VD
		* @return KafkaRDD实例
		*/
	def createRDD[
	K: ClassTag,
	V: ClassTag,
	KD <: Decoder[K]: ClassTag,
	VD <: Decoder[V]: ClassTag](
		sc: SparkContext,
		kafkaParams: Map[String, String],
		offsetRanges: Array[OffsetRange]
	):RDD[(K, V)]  = {
		val messageHandler =  (mmd: MessageAndMetadata[K, V]) => (mmd.key(), mmd.message())
		val kc = new KafkaCluster(kafkaParams)
		val leaders = leadersForRanges(kc, offsetRanges)
		new KafkaRDD[K, V, KD,  VD,(K, V)](sc, kafkaParams, offsetRanges, leaders, messageHandler)
	}

	/**
		* 不使用Receiver，而是直接根据偏移量范围直接创建对应的Kafka，该
		* 流能够确保kafka中的消息仅仅会被操作一次(Exactly Once)
		* @param ssc
		* @param kafkaParams
		* @param fromOffsets
		* @param messageHandler
		* @tparam K
		* @tparam V
		* @tparam KD
		* @tparam VD
		* @tparam R
		* @return
		*/
	def createDirectStream[
	K: ClassTag,
	V: ClassTag,
	KD <: Decoder[K]: ClassTag,
	VD <: Decoder[V]: ClassTag,
	R: ClassTag] (
		 ssc: StreamingContext,
		 kafkaParams: Map[String, String],
		 fromOffsets: Map[TopicAndPartition, Long],
		 messageHandler: MessageAndMetadata[K, V] => R
	 ): InputDStream[R] = {
		val cleanedHandler = ssc.sc.clean(messageHandler)
		new DirectKafkaInputDStream[K, V, KD, VD, R](
			ssc, kafkaParams, fromOffsets, cleanedHandler)
	}

	def createDirectStream[
	K: ClassTag,
	V: ClassTag,
	KD <: Decoder[K]: ClassTag,
	VD <: Decoder[V]: ClassTag] (
		ssc: StreamingContext,
		kafkaParams: Map[String, String],
		topics: Set[String]
	): InputDStream[(K, V)] = {
		val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
		val kc = new KafkaCluster(kafkaParams)
		val fromOffsets = getFromOffsets(kc, kafkaParams, topics)
		new DirectKafkaInputDStream[K, V, KD, VD, (K, V)](
			ssc, kafkaParams, fromOffsets, messageHandler)
	}

	private[kafka] def getFromOffsets(
		 kc: KafkaCluster,
		 kafkaParams: Map[String, String],
		 topics: Set[String]
	 ): Map[TopicAndPartition, Long] = {
		val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
		val result = for {
			topicPartitions <- kc.getPartitions(topics).right
			leaderOffsets <- (if (reset == Some("smallest")) {
				kc.getEarliestLeaderOffset(topicPartitions)
			} else {
				kc.getLatestLeadereOffset(topicPartitions)
			}).right
		} yield {
			leaderOffsets.map { case (tp, lo) =>
				(tp, lo.offset)
			}
		}
		KafkaCluster.checkErrors(result)
	}


}
