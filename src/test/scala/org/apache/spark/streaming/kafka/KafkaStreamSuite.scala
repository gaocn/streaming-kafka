package org.apache.spark.streaming.kafka

import java.util.concurrent.ConcurrentHashMap

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Microseconds, Span}

import scala.util.Random

class KafkaStreamSuite extends SparkFunSuite with Eventually with BeforeAndAfterAll{
	private var ssc: StreamingContext = _
	private var kafkaTestUtils: KafkaTestUtils = _

	override def beforeAll(): Unit = {
		kafkaTestUtils = new KafkaTestUtils
		kafkaTestUtils.setup()
	}

	override def afterAll(): Unit = {
		if (ssc != null) {
			ssc.stop()
			ssc = null
		}
		if (kafkaTestUtils != null) {
			kafkaTestUtils.teardown()
			kafkaTestUtils = null
		}
	}


	test("test KafkaInputDStream"){
		val conf = new SparkConf()
			.setMaster("local[4]")
  		.set("spark.streaming.receiver.writeAheadLog.enable", "false")
			.setAppName(this.getClass.getName)
		ssc = new StreamingContext(conf, Milliseconds(500))

		//测试ReliableKafkaReceiver
		//ssc.checkpoint("E:\\Users")

		val topic = "topic1"
//		val sent = Map("a" -> 2, "b" -> 1, "c" -> 2)
		val sent = Map( "b" -> 1)
		kafkaTestUtils.createTopic(topic)
		kafkaTestUtils.sendMessage(topic, sent)

		val kafkaParams = Map(
			"zookeeper.connect" -> kafkaTestUtils.zkAddress,
			"group.id" -> s"test-consumer-${Random.nextInt(10000)}",
			"auto.offset.reset" -> "smallest"
		)
		val stream = GKafkaUtils.createDStream[String, String, StringDecoder, StringDecoder](
			ssc,  kafkaParams, Map(topic -> 1), StorageLevel.MEMORY_ONLY
		)

		val result = new ConcurrentHashMap[String, Long]()

		stream.map(_._2).countByValue().foreachRDD{rdd =>
			val ret = rdd.collect()
			ret.toMap.foreach{kv =>
				logInfo(s"Streaming作业中统计接收到的记录：${kv}")
				val count = result.getOrDefault(kv._1, 0) + kv._2
				result.put(kv._1, count)
			}
		}

		ssc.start()
		ssc.awaitTermination()

		eventually(timeout(Span(10000, Microseconds) ), interval(Span(100, Microseconds))){
			logInfo(s"Streaming应用程序统计结果：${result}")
			sent.foreach{case (k, v) =>
				assert(result.get(k) == v)
			}
		}
	}



}
