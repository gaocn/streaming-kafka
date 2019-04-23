package org.apache.spark.streaming.kafka.receiver

import java.util.Properties
import java.util.concurrent.{ConcurrentHashMap, ThreadPoolExecutor}

import kafka.common.TopicAndPartition
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector, KafkaStream}
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import kafka.utils.{VerifiableProperties, ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.receiver.{BlockGenerator, BlockGeneratorListener, Receiver}
import org.apache.spark.util.ThreadUtils
import org.apache.spark.{Logging, SparkEnv}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.{ClassTag, classTag}

/**
	* 可靠性，能够将接收的数据存储在BlockManager中，保证不存在数据丢
	* 失，默认该功能是关闭的，即默认是KafkaReceiver。通过配置：
	*   spark.streaming.receiver.writeAheadLog.enable=true
	* 实现可靠kafka数据读取操作。
	*
	* ReliableKafkaReceiver与KafkaReceiver的区别：
	* 前者自己管理topic-partition/offset元数据，并且当数据已经被成功
	* 的存储在WAL Log中时才更新偏移量。由于偏移量只有在数据被成功存储时
	* 才会更新，因此就避免了使用后者常出现的数据丢失问题。
	*
	*
	* ReliableKafkaReceiver内部会将auto.commit.enable设置为false，
	* 因此手动在配置参数中进行配置是不会生效的。
	*
	* @param kafkaParam   kafka配置
	* @param topics       需要订阅主题
	* @param storageLevel 存储级别
	* @tparam K Kafka Record的键类型
	* @tparam V Kafka Record的值类型
	* @tparam U Kafka Record键的解码器
	* @tparam T Kafka Record值的解码器
	*/
private[streaming] class ReliableKafkaReceiver[
K: ClassTag,
V: ClassTag,
U <: Decoder[_] : ClassTag,
T <: Decoder[_] : ClassTag](
														 kafkaParam: Map[String, String],
														 topics: Map[String, Int],
														 storageLevel: StorageLevel
													 ) extends Receiver[(K, V)](storageLevel) with Logging {

	private val groupId = kafkaParam("group.id")
	private val AUTO_OFFSET_COMMIT = "auto.commit.enable"

	private def conf = SparkEnv.get.conf

	/**
		* 用于连接到kafka broker的高层 Connect API
		*/
	private var consumerConnector: ConsumerConnector = null

	/**
		* 连接到ZK，用于提交偏移量
		*/
	private var zkClient: ZkClient = null

	/**
		* 维护topic-partition与对应偏移量的状态，由于该变量会在同步代码
		* 块中被读写，因此mutable.HashMap不会存在并发问题
		*/
	private var topicPartitionOffsetMap: mutable.HashMap[TopicAndPartition, Long] = null

	/**
		* 支持并发的哈希表，用于保存stream block id与相关偏移量状态的快照
		*/
	private var blockOffsetMap: ConcurrentHashMap[StreamBlockId, Map[TopicAndPartition, Long]] = null

	/**
		* 在Receiver内部使用BlockGenerator以便更好地管理数据存储和偏移
		* 量提交
		*/
	private var blockGenerator: BlockGenerator = null

	/**
		* 对于接收来自多个主题的不同分区的消息，采用线程池进行处理
		*/
	private var messageHandlerThreadPool: ThreadPoolExecutor = null

	/**
		* 启动Receiver开始处理Kafka消息
		*/
	override def onStart(): Unit = {
		logInfo(s"启动Kafka Stream消费群组：${groupId}")

		topicPartitionOffsetMap = new mutable.HashMap[TopicAndPartition, Long]
		blockOffsetMap = new ConcurrentHashMap[StreamBlockId, Map[TopicAndPartition, Long]]

		//初始化，用于存储接收的kafka消息
		blockGenerator = supervisor.createBlockGenerator(new GeneratedBlockHandler)

		if (kafkaParam.contains(AUTO_OFFSET_COMMIT) && kafkaParam(AUTO_OFFSET_COMMIT) == "true") {
			logWarning(s"在ReliableKafkaReceiver中${AUTO_OFFSET_COMMIT}应该设置为false，	" +
				s"在ReliableKafkaReceiver里会自动设置为false而忽略用户的配置")
		}

		val props = new Properties()
		kafkaParam.foreach(KV => props.put(KV._1, KV._2))

		/** 自动设置为false，不能自动提交 */
		props.setProperty(AUTO_OFFSET_COMMIT, "false");
		val consumerConfig = new ConsumerConfig(props)

		assert(!consumerConfig.autoCommitEnable, "偏移量自动提交不能为true")

		logInfo(s"尝试连接ZooKeeper：${consumerConfig.zkConnect}")
		consumerConnector = Consumer.create(consumerConfig)
		logInfo(s"成功连接ZooKeeper：${consumerConfig.zkConnect}")

		zkClient = new ZkClient(consumerConfig.zkConnect, consumerConfig.zkSessionTimeoutMs, consumerConfig.zkConnectionTimeoutMs, ZKStringSerializer)

		/** 创建处理消息的线程池，每个分区对应一个线程 */
		messageHandlerThreadPool = ThreadUtils.newDaemonFixedThreadPool(topics.values.sum, "KafkaMessageHandler")

		logInfo(s"启动blockGenerator: ${blockGenerator}")
		blockGenerator.start()

		val keyDecoder = classTag[U].runtimeClass
			.getConstructor(classOf[VerifiableProperties])
  		.newInstance(consumerConfig.props)
  		.asInstanceOf[Decoder[K]]

		val valueDecoder = classTag[T].runtimeClass
			.getConstructor(classOf[VerifiableProperties])
			.newInstance(consumerConfig.props)
			.asInstanceOf[Decoder[V]]

		val topicMessageStreams = consumerConnector.createMessageStreams(topics, keyDecoder, valueDecoder)

		topicMessageStreams.values.foreach{streams =>
			streams.foreach{kstream=>
				messageHandlerThreadPool.submit(new MessageHandler(kstream))
			}
		}

	}

	/**
		* 关闭Receiver并释放资源
		*/
	override def onStop(): Unit = {
		if(messageHandlerThreadPool != null) {
			messageHandlerThreadPool.shutdown()
			messageHandlerThreadPool = null
		}

		if (consumerConnector != null) {
			consumerConnector.shutdown()
			consumerConnector = null
		}

		if (zkClient != null) {
			zkClient.close()
			zkClient = null
		}

		if (blockGenerator != null) {
			blockGenerator.stop()
			blockGenerator = null
		}

		if (topicPartitionOffsetMap != null) {
			topicPartitionOffsetMap.clear()
			topicPartitionOffsetMap = null
		}

		if (blockOffsetMap != null) {
			blockOffsetMap.clear()
			blockOffsetMap = null
		}
	}


	/** 保存接收到的一条kafka消息及其metadata */
	private def storeMessageAndMetadata(messgeAndMetadata:MessageAndMetadata[K, V])  = {
		val topicAndPartition = TopicAndPartition(messgeAndMetadata.topic, messgeAndMetadata.partition)
		val data  = (messgeAndMetadata.key, messgeAndMetadata.message)
		val metadata = (topicAndPartition, messgeAndMetadata.offset)
		blockGenerator.addDataWithCallback(data, metadata)
	}

	/** 更新要提交的偏移量 */
	private def updateOffset(topicAndPartition: TopicAndPartition, offset:Long) = {
		topicPartitionOffsetMap.put(topicAndPartition, offset)
	}

	/**
		* 当产生一个块时，将当前所有主题分区与偏移量的对象关系进行一次备份
		* （snapshot，快照）
		* @param blockId 块Id
		*/
	private def rememberBlockOffsets(blockId: StreamBlockId) ={
		val offsetSnapshot = topicPartitionOffsetMap.toMap
		blockOffsetMap.put(blockId, offsetSnapshot)
		topicPartitionOffsetMap.clear()
	}


	/**
		* 持久化接收的数据同时更新偏移量
		* 1、将已准备好持久化到的block进行存储同时提交对应偏移量；
		* 2、若存储失败，会尝试3次，若最终失败则停止Receiver
		*/
	private def storeBlockAndCommitOffset(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_]) = {
		var retryCount = 0
		var pushed = false
		var exception: Exception = null

		while (!pushed && retryCount <= 3) {
			try {
				store(arrayBuffer.asInstanceOf[mutable.ArrayBuffer[(K, V)]])
				pushed = true
			} catch {
				case ex:Exception =>
					retryCount += 1
					exception = ex
			}
		}

		if (pushed) {
			//若block存储成功，则提交偏移量
			Option(blockOffsetMap.get(blockId)).foreach(commitOffset)
			blockOffsetMap.remove(blockId)
		} else {
			stop("持久化block到spark时失败，停止Receiver！", exception)
		}
	}

	/**
		* 提交主题分区对应的偏移量
		* 注意：基于Kafka 0.8.X的偏移量元数据存放在ZK中。
		*/
	def commitOffset(offsetMap: Map[TopicAndPartition, Long]):Unit = {
		if(zkClient == null) {
			val thrown = new IllegalStateException("zkClient实例为空，将停止Receiver")
			stop("zkClient实例为没有实例化，不能提交偏移量", thrown)
			return
		}

		for ((topicAndPartition, offset) <- offsetMap) {
			try {
				val topicDirs = new ZKGroupTopicDirs(groupId, topicAndPartition.topic)
				val zkPath = s"${topicDirs}/${topicAndPartition.partition}"
				ZkUtils.updatePersistentPath(zkClient, zkPath, offset.toString)
			} catch {
				case e: Exception =>
					logWarning(s"提交主题分区${topicAndPartition}的偏移量：${offset}的过程中出错：${e}")
			}
			logInfo(s"成功提交主题分区${topicAndPartition}的偏移量：${offset}")
		}
	}

	/**
		* 用于处理接收到Kafka消息
		* @param value
		*/
	private final class MessageHandler(value: KafkaStream[K, V]) extends Runnable {
		override def run(): Unit = {
			while (!isStopped) {
				try {
					val streamIter = value.iterator()
					while (streamIter.hasNext()) {
						val msg = streamIter.next()
						storeMessageAndMetadata(msg)
						logInfo(s"接收消息：${msg}")
					}
				} catch {
					case e: Exception =>
						reportError(s"处理接收的Kafka消息时出错:${e.getMessage}", e)
				}
			}
		}
	}
	/**
		* 监听数据是否写入成功，根据不同的写入状态更新偏移量
		*/
	private final class GeneratedBlockHandler extends BlockGeneratorListener {
		//成功缓存数据
		override def onAddData(data: Any, metadata: Any): Unit = {
			//数据已经成功添加到BlockGenerator，更新偏移量
			if(metadata != null) {
				val (topicAndPartition, offset) = metadata.asInstanceOf[(TopicAndPartition, Long)]
				logInfo(s"数据成功缓存，更新${topicAndPartition}的偏移量")
				updateOffset(topicAndPartition, offset)
			}
		}
		//成功建立Block
		override def onGenerateBlock(blockId: StreamBlockId): Unit = {
			//对产生的Block对应的偏移量已经快照，以便存储
			rememberBlockOffsets(blockId)
			logInfo(s"根据接收的数据，成功创建Block")
		}

		override def onPushBlock(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_]): Unit = {
			storeBlockAndCommitOffset(blockId, arrayBuffer)
			logInfo(s"尝试调用BlockManager持久化数据，并提交偏移量")
		}

		override def onError(message: String, throwable: Throwable): Unit = {
			reportError(message, throwable)
		}
	}

}

