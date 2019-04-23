package org.apache.spark.streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.receiver.ReliableKafkaReceiver
import org.apache.spark.streaming.receiver.ReceiverSupervisorImpl
import org.junit.Test


class TestReliableKafkaReceiver {
	var receiver:ReliableKafkaReceiver[String, String, StringDecoder, StringDecoder] = null
	@Test
	def  testReliableKafkaReceiver():Unit = {

		val kafkaParams = Map[String, String](
			"bootstrap.servers"->"10.230.135.124:9093,10.230.135.125:9093",
			"zookeeper.connect"->"10.230.135.124:2181,10.230.135.125:2181",
			"group.id"->"kafka-cluster-test"
		)
		val topics =  Map(
			"Log_BJS_Log" -> 10
		)

		val conf = new SparkConf()
			.setMaster("local")
			.setAppName("Kafka Receiver Test")
		val sc = new SparkContext(conf)

		val ssc  = new StreamingContext(sc,Durations.seconds(5))

		receiver = new ReliableKafkaReceiver(kafkaParams, topics, StorageLevel.MEMORY_ONLY)
		receiver.attachSupervisor(new ReceiverSupervisorImpl(receiver, ssc.env, new Configuration(),Option("D:\\TFSX2015\\OBS\\dev\\streaming-kafka\\target")))
		receiver.onStart()
		println("启动接收器")
	}
}
