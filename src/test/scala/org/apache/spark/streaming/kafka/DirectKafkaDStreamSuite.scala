package org.apache.spark.streaming.kafka

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.domain.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.kafka.rdd.KafkaRDD
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted, StreamingListenerBatchStarted, StreamingListenerBatchSubmitted}
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Time}
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkConf, SparkContext, SparkFunSuite}
import org.scalatest.concurrent.Eventually
import scala.concurrent.duration._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class DirectKafkaDStreamSuite extends SparkFunSuite with BeforeAndAfter with BeforeAndAfterAll with Eventually with Logging{

	val sparkConf = new SparkConf()
  	.setMaster("local[2]")
  	.setAppName("DirectKafkaDStreamSuite")

	private var sc: SparkContext = _
	private var ssc: StreamingContext = _
	private var testDir: File =  _

	private var kafkaTestUtils: KafkaTestUtils = _

	override def beforeAll(): Unit = {
		kafkaTestUtils = new KafkaTestUtils
		kafkaTestUtils.setup()
	}

	override def afterAll(): Unit = {
		if (kafkaTestUtils != null) {
			kafkaTestUtils. teardown()
		}
	}

	after{
		if (ssc != null) {
			ssc.stop()
			ssc =  null
		}

		if (sc != null) {
			sc.stop()
			sc = null
		}

		if (testDir != null) {
			Utils.deleteRecursively(testDir)
		}
	}

	test("basic stream receiving with multiple topics and smallest starting offset"){
		val topics = Set("basic1", "basic2", "basic3")
		val data = Map("a" -> 1, "b" -> 1, "c" -> 1)
		topics.foreach{ topic =>
			kafkaTestUtils.createTopic(topic)
			kafkaTestUtils.sendMessage(topic, data)
		}

		val totalSend = data.values.sum * topics.size
		val kafkaParams = Map(
			"metadata.broker.list" -> kafkaTestUtils.brokerAddress,
			"zookeeper.connect" -> kafkaTestUtils.zkAddress,
			"auto.offset.reset" -> "smallest",
			"group.id" -> "DirectKafkaDStreamGroup"
		)

		ssc = new StreamingContext(sparkConf, Milliseconds(200))
		val stream = withClue("创建Direct Stream失败") {
			GKafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
		}

		val allReceived = new ArrayBuffer[(String, String)] with mutable.SynchronizedBuffer[(String, String)]

		var offsetRanges = Array[OffsetRange]()

		stream.transform{rdd =>
			offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
			rdd
		}.foreachRDD{rdd=>
			for (o <- offsetRanges) {
				logInfo(s"当前RDD的详细信息：${o.topic}, ${o.partition}, ${o.fromOffset}, ${o.untilOffset}")
			}

			val collected = rdd.mapPartitionsWithIndex{(i, iter)=>
				val off = offsetRanges(i)
				val all = iter.toSeq
				val partSize =  all.size
				val rangeSize = off.untilOffset - off.fromOffset
				Iterator((partSize, rangeSize))
			}.collect

			collected.foreach{case (partSize, rangeSize) =>
				assert(partSize == rangeSize, "偏移量范围应该与分区中记录数相等")
			}
		}

		stream.foreachRDD{rdd => allReceived ++= rdd.collect}
		ssc.start()

		eventually(timeout(20000 milliseconds), interval(200 milliseconds)) {
			assert(allReceived.size == totalSend, "发送消息数目与接收消息数目不一致！")
		}

		ssc.stop()
	}

	private def getOffsetRanges[K, V](kafkaStream: DStream[(K, V)]):Seq[(Time, Array[OffsetRange])] = {
		kafkaStream.generatedRDDs.mapValues{rdd =>
			rdd.asInstanceOf[KafkaRDD[K,V,_,_,(K, V)]].offsetRanges
		}.toSeq.sortBy(_._1)
	}
}

object DirectKafkaDStreamSuite  {
	val collectedData = new ArrayBuffer[String]() with mutable.SynchronizedBuffer[String]
	@volatile var total = -1L

	class InputInfoCollector extends StreamingListener {

		val numRecordsSubmitted = new AtomicLong(0L)
		val numRecordsStarted= new AtomicLong(0L)
		val numRecordsCompleted = new AtomicLong(0L)

		override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
			numRecordsStarted.addAndGet(batchStarted.batchInfo.numRecords)
		}

		override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
			numRecordsSubmitted.addAndGet(batchSubmitted.batchInfo.numRecords)
		}

		override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
			numRecordsCompleted.addAndGet(batchCompleted.batchInfo.numRecords)
		}
	}
}

private[streaming] class ConstantEstimator(@volatile private var rate: Long) extends RateEstimator{
	def updateRate(newRate: Long):Unit = {
		rate = newRate
	}

	override def compute(time: Long, elements: Long, processingDelay: Long, schedulingDelay: Long): Option[Double] = Some(rate)
}