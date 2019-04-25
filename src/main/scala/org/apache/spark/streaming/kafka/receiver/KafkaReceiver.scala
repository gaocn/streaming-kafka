package org.apache.spark.streaming.kafka.receiver

import java.util.Properties

import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector, KafkaStream}
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.util.ThreadUtils

import scala.reflect.{ClassTag, classTag}

/**
	* 采用pull方式从Kafka中拉取数据，默认自动提交偏移量
	*/
private[streaming] class KafkaReceiver[
K: ClassTag,
V: ClassTag,
U <: Decoder[_] : ClassTag,
T <: Decoder[_] : ClassTag
](
	 kafkaParams: Map[String, String],
	 topics: Map[String, Int],
	 storageLevel: StorageLevel
 ) extends Receiver[(K, V)](storageLevel) with Logging {
	//Kafka 消费者
	var consumerConnector: ConsumerConnector = null

	override def onStop(): Unit = {
		if(consumerConnector != null) {
			consumerConnector.shutdown();
			consumerConnector = null
		}
	}

	override def onStart(): Unit = {
		logInfo(s"启动KafkaReceiver，消费群组为：${kafkaParams.get("group.id")}")

		val props = new Properties()
		kafkaParams.foreach(KV => props.put(KV._1, KV._2))
		val conf = new ConsumerConfig(props)

		logInfo(s"开始连接Kafka服务器所在ZK: ${conf.zkConnect}")
		consumerConnector = Consumer.create(conf)
		logInfo(s"成功连接Kafka服务器所在ZK: ${conf.zkConnect}")

		val keyDecoder = classTag[U].runtimeClass.getConstructor(classOf[VerifiableProperties])
  		.newInstance(conf.props).asInstanceOf[Decoder[K]]
		val valueDecoder = classTag[T].runtimeClass.getConstructor(classOf[VerifiableProperties])
			.newInstance(conf.props).asInstanceOf[Decoder[V]]

		val topicMessageStreams = consumerConnector.createMessageStreams(topics, keyDecoder, valueDecoder)

		val excutorPool = ThreadUtils.newDaemonFixedThreadPool(topics.values.sum, "KafkaReceiverHandler")

		try {
			topicMessageStreams.values.foreach(streams => {
				streams.foreach { stream =>
					logInfo(s"为主题每个分区分别创建一个线程用于消费数据：${stream}")
					excutorPool.submit(new MessageHandler(stream))
				}
			})
		} finally {
			excutorPool.shutdown()
		}
	}

	private class  MessageHandler(stream: KafkaStream[K,V]) extends Runnable {
		override def run(): Unit = {
			try {
				val streamIter = stream.iterator()
				while (streamIter.hasNext()) {
					val messageAndMetadata = streamIter.next()
					logInfo(s"KafkaReceiver接收到数据:${messageAndMetadata}，开始处理")
					store((messageAndMetadata.key(), messageAndMetadata.message()))
				}
			} catch {
				case e: Exception  => reportError("处理异常失败退出", e)
			}
		}
	}

}
