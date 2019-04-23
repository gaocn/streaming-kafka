package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.kafka.rdd.KafkaRDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class TestKafkaRDD {
	var rdd:KafkaRDD[String, String, StringDecoder, StringDecoder, String] = null

	@Test
	def testKafkaRDD(): Unit = {
		val kafkaParams = Map[String, String](
			"bootstrap.servers"->"10.230.135.124:9093,10.230.135.125:9093",
			"zookeeper.connect"->"10.230.135.124:2181,10.230.135.125:2181",
			"group.id"->"kafka-cluster-test"

		)
		val conf = new SparkConf()
			.setMaster("local")
			.setAppName("Kafka RDD Test")
		val sc = new SparkContext(conf)

		val kc =KafkaCluster(kafkaParams)

		val topic = "Log_BJS_Log"
		val tp = new TopicAndPartition(topic, 0)
		val fromOffsets  = Map(
			tp -> 0L
		)

		var untilOffsets:Map[TopicAndPartition, LeaderOffset]= kc.getLeaderOffset(Set(tp), System.currentTimeMillis()).fold(
			err=> null.asInstanceOf[Map[TopicAndPartition, LeaderOffset]],
			tpLOMap => tpLOMap)

		val messageHandler = (mm:MessageAndMetadata[String, String]) => mm.message()

		val leader:Map[TopicAndPartition, (String, Int)] = kc.findLeaders(Set(tp)).fold(
			err=> null.asInstanceOf[Map[TopicAndPartition, (String, Int)]],
			leader=>leader
		)
		rdd = KafkaRDD(sc, kafkaParams,fromOffsets, untilOffsets,messageHandler)

		rdd.take(10).foreach(println)
	}
}
