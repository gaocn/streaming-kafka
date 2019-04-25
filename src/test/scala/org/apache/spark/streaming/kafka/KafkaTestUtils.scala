package org.apache.spark.streaming.kafka

import java.io.File
import java.net.InetSocketAddress
import java.util.Properties

import kafka.admin.AdminUtils
import kafka.api.Request
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.Time
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkConf}
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}

import scala.annotation.tailrec
import scala.concurrent.TimeoutException
import scala.util.control.NonFatal

private[kafka] class KafkaTestUtils extends Logging {
	//ZK相关配置
	private val zkHost = "localhost"
	private var zkPort = 0
	private val zkConnectionTimeout = 60000
	private val zkSessionTimeout = 6000

	private var zookeeper: EmbeddedZookeeper = _

	// Kafka Broker相关配置
	private val brokerHost = "localhost"
	private var brokerPort = 9092
	private var brokerConfig: KafkaConfig = _

	private var server: KafkaServer = _


	// ZK 客户端
	private var zkClient: ZkClient = _

	// Kafka 生产者
	private var producer: Producer[String, String] = _

	private var zkReady = false
	private var brokerReady = false

	def zkAddress: String = {
		assert(zkReady, "ZK尚未启动或已经关闭，无法获取地址")
		s"${zkHost}:${zkPort}"
	}

	def brokerAddress: String = {
		assert(brokerReady, "borker尚未启动或已经关闭，无法获取地址")
		s"${brokerHost}:${brokerPort}"
	}

	def zookeeperClient: ZkClient = {
		assert(zkReady, "ZK尚未启动或已经关闭，无法获取ZkClient实例")
		Option(zkClient).getOrElse(throw new IllegalStateException("ZK尚未完成出初始化"))
	}

	private def setupEmbeddedZookeeper() = {
		zookeeper = new EmbeddedZookeeper(s"${zkHost}:${zkPort}")
		zkPort = zookeeper.actualPort
		zkClient = new ZkClient(s"${zkPort}:${zkPort}", zkSessionTimeout, zkConnectionTimeout)
		zkReady = true
	}

	private def setupKafkaServer() = {
		assert(zkReady, "启动Kafka前必须确保ZooKeeper先启动")

		Utils.startServiceOnPort(brokerPort, port => {
			brokerPort = port
			brokerConfig = new KafkaConfig(brokerConfiguration)
			server = new KafkaServer(brokerConfig)
			server.startup()
			(server, port)
		}, new SparkConf(), "KafkaBroker")
		brokerReady = true
	}

	private def brokerConfiguration: Properties = {
		val props = new Properties()
		props.put("broker.id", "0")
		props.put("host.name", "localhost")
		props.put("port", brokerPort.toString)
		props.put("log.dir", Utils.createTempDir().getAbsolutePath)
		props.put("zookeeper.connect", zkAddress)
		props.put("log.flush.interval.messages", "1")
		props.put("replica.socket.timeout.ms", "1500")
		props
	}

	def setup() = {
		setupEmbeddedZookeeper()
		setupKafkaServer()
	}

	def teardown() = {
		zkReady = false
		brokerReady = false

		if (producer != null) {
			producer.close()
			producer = null
		}

		if (server != null) {
			server.shutdown()
			server = null
		}

		brokerConfig.logDirs.foreach { f => Utils.deleteRecursively(new File(f)) }

		if (zkClient != null) {
			zkClient.close()
			zkClient = null
		}

		if (zookeeper != null) {
			zookeeper.shutdown()
			zookeeper = null
		}
	}

	def createTopic(topic: String): Unit = {
		AdminUtils.createTopic(zkClient, topic,1, 1)
		waitUntilMetadataIsPropagated(topic, 0)
	}

	def  sendMessage(topic:String, messageToFreq: Map[String, Int]):Unit = {
		val message = messageToFreq.flatMap{case (s, freq) =>
			Seq.fill(freq)(s)
		}.toArray
		sendMessage(topic, message)
	}

	def sendMessage(topic: String, message: Array[String]):Unit = {
		producer = new Producer[String, String](new ProducerConfig(producerConfiguration))
		producer.send(message.map{new KeyedMessage[String, String](topic, _)}: _*)
		producer.close()
		producer = null
	}

	private def producerConfiguration: Properties = {
		val props = new Properties();
		props.put("metadata.broker.list", brokerAddress)
		props.put("serializer.class", classOf[String].getName)
		//等待所有ISR确认后，返回确认响应
		props.put("request.required.acks", "-1")
		props
	}

	private def waitUntilMetadataIsPropagated(topic: String, partition: Int): Unit = {
		def isPropagated = server.apis.metadataCache.getPartitionInfo(topic, partition) match {
			case Some(partitionStateInfo) =>
				val leaderAndInSyncReplicas = partitionStateInfo.leaderIsrAndControllerEpoch.leaderAndIsr

				ZkUtils.getLeaderAndIsrForPartition(zkClient, topic, partition).isDefined &&
				Request.isValidBrokerId(leaderAndInSyncReplicas.leader) &&
				leaderAndInSyncReplicas.isr.size >= 1
			case _ =>
				false
		}
		eventually(Time(10000), Time(100)){
			assert(isPropagated, s"分区[${topic}, ${partition}]的元数据在等待的时间内尚未在集群中同步")
		}
	}

	/**
		* scalatest eventually的简单实现，为了避免引入额外的测试包依赖
		*/
	private def eventually[T](timeout: Time, interval: Time)(func: => T): T = {
		def makeAttempt(): Either[Throwable, T] = {
			try {
				Right(func)
			} catch {
				case e if NonFatal(e) => Left(e)
			}
		}
		val startTime = System.currentTimeMillis()
		@tailrec
		def tryAgain(attempt: Int): T = {
			makeAttempt() match {
				case Right(result) => result
				case Left(e) =>
					val duration = System.currentTimeMillis() - startTime
					if (duration < timeout.milliseconds) {
						Thread.sleep(interval.milliseconds)
					} else {
						throw new TimeoutException(e.getMessage)
					}
					tryAgain(attempt + 1)
			}
		}
		tryAgain(1)
	}

	/**
		* 内嵌ZooKeeper，用于测试
		*/
	private class EmbeddedZookeeper(val zkConnect: String) {
		val snapshotDir = Utils.createTempDir()
		val logDir = Utils.createTempDir()

		val zookeeper = new ZooKeeperServer(snapshotDir, logDir, 500)
		val (ip, port) = {
			val splits = zkConnect.split(":")
			(splits(0), splits(1).toInt)
		}

		val factory = new NIOServerCnxnFactory()
		factory.configure(new InetSocketAddress(ip, port), 16)
		factory.startup(zookeeper)

		val actualPort = factory.getLocalPort

		def shutdown() {
			factory.shutdown();
			Utils.deleteRecursively(snapshotDir)
			Utils.deleteRecursively(logDir)
		}
	}

}
