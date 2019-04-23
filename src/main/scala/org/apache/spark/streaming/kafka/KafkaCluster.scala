package org.apache.spark.streaming.kafka

import java.util.Properties

import kafka.api._
import kafka.common.{ErrorMapping, OffsetAndMetadata, OffsetMetadataAndError, TopicAndPartition}
import kafka.consumer.{ConsumerConfig, SimpleConsumer}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.NonFatal

/**
	* 提供用于和Kafka集群交互获取信息的方法
	*/
private[kafka] class KafkaCluster(val kafkaParam: Map[String, String]) extends Serializable {
	import KafkaCluster.{Err, LeaderOffset, SimpleConsumerConfig}
	/*不需要持久化*/
	@transient private var _config: SimpleConsumerConfig = null

	def config: SimpleConsumerConfig = this.synchronized {
		if (_config == null) {
			_config = SimpleConsumerConfig(kafkaParam)
		}
		_config
	}

	/**
		* 创建连接到指定broker的消费者
		*/
	def connect(host: String, port: Int): SimpleConsumer = {
		new SimpleConsumer(host, port, 60000, config.socketReceiveBufferBytes, config.clientId)
	}

	/**
		* 连接到指定的broker上，并执行指定的操作
		*
		* @param brokers 指定消息代理地址
		* @param err     若执行过程中出错，则将错误添加到err列表中
		* @param func    操作
		*/
	def withBrokers(brokers: Iterable[(String, Int)], err: Err)(func: SimpleConsumer => Any): Unit = {
		brokers.foreach { broker =>
			var consumer: SimpleConsumer = null
			try {
				consumer = connect(broker._1, broker._2)
				func(consumer)
			} catch {
				case NonFatal(e) => err.append(e)
			} finally {
				if (consumer != null) {
					consumer.close();
				}
			}
		}
	}

	/**
		* 连接到指定主题分区ID上
		*
		* @param topic     主题名
		* @param partition 主题特定分区
		* @return
		*/
	def connectLeader(topic: String, partition: Int): Either[Err, SimpleConsumer] = {
		findLeader(topic, partition).right.map(hostPort => connect(hostPort._1, hostPort._2))
	}

	/**
		* 通过Metadata API获取特定主题分区所在的broker
		*
		* @param topic     主题名
		* @param partition 主题特定分区
		* @return
		*/
	def findLeader(topic: String, partition: Int): Either[Err, (String, Int)] = {
		val req = TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, 0, config.clientId, Seq(topic))
		val errs = new Err
		withBrokers(Random.shuffle(config.seedBrokers), errs) { consumer =>
			val resp = consumer.send(req)
			resp.topicsMetadata.find(_.topic == topic).flatMap { topicMetaData =>
				topicMetaData.partitionsMetadata.find(_.partitionId == partition)
			}.foreach { partitionMetadata =>
				partitionMetadata.leader.foreach { leader =>
					return Right((leader.host, leader.port))
				}
			}
		}
		Left(errs)
	}

	def findLeaders(topicAndPartitions: Set[TopicAndPartition]): Either[Err, Map[TopicAndPartition, (String, Int)]] = {
		val topics = topicAndPartitions.map(_.topic)
		val resp = getPartitionMetadata(topics)
		val answer = resp.right.flatMap { tmSet =>
			val leaderMap = tmSet.flatMap { tm =>
				tm.partitionsMetadata.flatMap { pm =>
					val tp = TopicAndPartition(tm.topic, pm.partitionId)
					if (topicAndPartitions.contains(tp)) {
						pm.leader.map { broker =>
							tp -> (broker.host, broker.port)
						}
					} else {
						None
					}
				}
			}.toMap

			if (leaderMap.keys.size == topicAndPartitions.size) {
				Right(leaderMap)
			} else {
				val missing = topicAndPartitions.diff(leaderMap.keySet)
				val errs = new Err
				errs.append(new Exception(s"Couldn't find leaders for ${missing}"))
				Left(errs)
			}
		}
		answer
	}

	def getPartitionMetadata(topics: Set[String]): Either[Err, Set[TopicMetadata]] = {
		val req = TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, 0, config.clientId, topics.toSeq)
		val errs = new Err
		withBrokers(Random.shuffle(config.seedBrokers), errs) { consumer =>
			val resp = consumer.send(req)
			val respErrs = resp.topicsMetadata.filter(tm => tm.errorCode != ErrorMapping.NoError)
			if (respErrs.isEmpty) {
				return Right(resp.topicsMetadata.toSet)
			} else {
				respErrs.foreach { err =>
					val cause = ErrorMapping.exceptionFor(err.errorCode)
					val msg = s"Error getting partition metadata for '${err.topic}'. Does the topic exists?"
					errs.append(new Exception(msg, cause))
				}
			}
		}
		Left(errs)
	}

	def getPartitions(topics: Set[String]): Either[Err, Set[TopicAndPartition]] = {
		getPartitionMetadata(topics).right.map { tmSet =>
			tmSet.flatMap { tm =>
				tm.partitionsMetadata.map { pm =>
					TopicAndPartition(tm.topic, pm.partitionId)
				}
			}
		}
	}

	// Leader Offset API
	//
	def getLatestLeadereOffset(topicAndPartitions: Set[TopicAndPartition]): Either[Err, Map[TopicAndPartition, LeaderOffset]] = {
		getLeaderOffset(topicAndPartitions, OffsetRequest.LatestTime)
	}

	def getEarliestLeaderOffset(topicAndPartitions: Set[TopicAndPartition]): Either[Err, Map[TopicAndPartition, LeaderOffset]] = {
		getLeaderOffset(topicAndPartitions, OffsetRequest.EarliestTime)
	}

	def getLeaderOffset(topicAndPartitions: Set[TopicAndPartition], before: Long): Either[Err, Map[TopicAndPartition, LeaderOffset]] = {
		getLeaderOffset(topicAndPartitions, before, 1).right.map { r =>
			r.map { kv =>
				//map values isn't serializable
				kv._1 -> kv._2.head
			}
		}
	}

	/**
		* @param topicAndPartition
		* @param before
		* @param maxNumOffsets
		* @return Map[TopicAndPartition, Set[LeaderOffset]]]
		**/
	def getLeaderOffset(topicAndPartitions: Set[TopicAndPartition], before: Long, maxNumOffsets: Int): Either[Err, Map[TopicAndPartition, Seq[LeaderOffset]]] = {
		findLeaders(topicAndPartitions).right.flatMap { tpToLeader =>
			val leaderToTP = flip(tpToLeader)
			val leaders = leaderToTP.keys
			var result = Map[TopicAndPartition, Seq[LeaderOffset]]()
			val errs = new Err

			withBrokers(leaders, errs) { consumer =>
				val partitionsToGetOffset = leaderToTP((consumer.host, consumer.port))
				val reqMap = partitionsToGetOffset.map { tp =>
					tp -> PartitionOffsetRequestInfo(before, maxNumOffsets)
				}.toMap
				val req = OffsetRequest(reqMap)
				val resp = consumer.getOffsetsBefore(req)
				val respMap = resp.partitionErrorAndOffsets
				partitionsToGetOffset.foreach { tp =>
					respMap.get(tp).foreach { por =>
						if (por.error == ErrorMapping.NoError) {
							if (por.offsets.nonEmpty) {
								result += tp -> por.offsets.map { offset => LeaderOffset(consumer.host, consumer.port, offset) }
							} else {
								errs.append(new Exception(s"Empty offsets for ${tp}, is ${before} before log beginning"))
							}
						} else {
							errs.append(ErrorMapping.exceptionFor(por.error))
						}
					}
				}
			}
			if (result.keys.size == topicAndPartitions.size) {
				return Right(result)
			}
			val missing = topicAndPartitions.diff(result.keySet)
			errs.append(new Exception(s"Couldn't find leander offsets for ${missing}"))
			Left(errs)
		}
	}

	def flip[K, V](m: Map[K, V]): Map[V, Seq[K]] = {
		m.groupBy(_._2).map { kv =>
			kv._1 -> kv._2.keys.toSeq
		}
	}

	// Consumer Offset API
	//
	//0 indicate using original ZK backed api
	val defaultConsumerApiVersion: Short = 0

	def getConsumerOffsets(groupId: String, topicAndPartitions: Set[TopicAndPartition]): Either[Err, Map[TopicAndPartition, Long]] = {
		getConsumerOffsets(groupId, topicAndPartitions, defaultConsumerApiVersion)
	}

	def getConsumerOffsets(groupId: String, topicAndPartitions: Set[TopicAndPartition], consumerApiVersion: Short): Either[Err, Map[TopicAndPartition, Long]] = {
		getConsumerOffsetMetadata(groupId, topicAndPartitions, consumerApiVersion).right.map { r =>
			r.map { kv =>
				kv._1 -> kv._2.offset
			}
		}
	}

	def getConsumerOffsetMetadata(groupId: String, topicAndPartitions: Set[TopicAndPartition]): Either[Err, Map[TopicAndPartition, OffsetMetadataAndError]] = {
		getConsumerOffsetMetadata(groupId, topicAndPartitions, defaultConsumerApiVersion)
	}

	def getConsumerOffsetMetadata(groupId: String, topicAndPartitions: Set[TopicAndPartition], consumerApiVersion: Short): Either[Err, Map[TopicAndPartition, OffsetMetadataAndError]] = {
		var result = Map[TopicAndPartition, OffsetMetadataAndError]()
		val req = OffsetFetchRequest(groupId, topicAndPartitions.toSeq, consumerApiVersion)
		val errs = new Err

		withBrokers(Random.shuffle(config.seedBrokers), errs) { consumer =>
			val resp = consumer.fetchOffsets(req)
			val respMap = resp.requestInfo
			val needed = topicAndPartitions.diff(result.keySet)
			needed.foreach { tp =>
				respMap.get(tp).foreach { ome =>
					if (ome.error == ErrorMapping.NoError) {
						result += tp -> ome
					} else {
						errs.append(ErrorMapping.exceptionFor(ome.error))
					}
				}
			}
			if (result.keys.size == topicAndPartitions.size) {
				return Right(result)
			}
		}
		val missing = topicAndPartitions.diff(result.keySet)
		errs.append(new Exception(s"Couldn't find consumer offset for ${missing}"))
		Left(errs)
	}


	def setConsumerOffsets(groupId:String, offsets:Map[TopicAndPartition, Long]):Either[Err, Map[TopicAndPartition, Short]] = {
		setConsumerOffsets(groupId, offsets, defaultConsumerApiVersion)
	}

	def setConsumerOffsets(groupId:String, offsets:Map[TopicAndPartition, Long], consumerApiVersion: Short):Either[Err, Map[TopicAndPartition, Short]] = {
		val metadata = offsets.map{kv =>
			kv._1 -> OffsetAndMetadata(kv._2)
		}
		setConsumerOffsetMetadata(groupId, metadata, consumerApiVersion)
	}

	def setConsumerOffsetMetadata(groupId:String, metadata:Map[TopicAndPartition, OffsetAndMetadata]):Either[Err, Map[TopicAndPartition, Short]] = {
		setConsumerOffsetMetadata(groupId, metadata, defaultConsumerApiVersion)
	}

	def setConsumerOffsetMetadata(groupId:String, metadata:Map[TopicAndPartition, OffsetAndMetadata], consumerApiVersion: Short):Either[Err, Map[TopicAndPartition, Short]] = {
		var result = Map[TopicAndPartition, Short]()
		val req = OffsetCommitRequest(groupId,metadata, consumerApiVersion)
		val errs = new Err
		val topicAndPartitions = metadata.keySet

		withBrokers(Random.shuffle(config.seedBrokers), errs){consumer =>
			val resp = consumer.commitOffsets(req)
			val respMap = resp.commitStatus
			val needed = topicAndPartitions.diff(result.keySet)
			needed.foreach{tp =>
				respMap.get(tp).foreach{err =>
					if (err == ErrorMapping.NoError) {
						result += tp -> err
					} else {
						errs.append(ErrorMapping.exceptionFor(err))
					}
				}
			}
			if (result.keys.size == topicAndPartitions.size) {
				return Right(result)
			}
		}
		val missing = topicAndPartitions.diff(result.keySet)
		errs.append(new Exception(s"Couldn't set offset for ${missing}"))
		Left(errs)
	}

}

private[spark] object KafkaCluster {
	def apply(kafkaParam: Map[String, String]): KafkaCluster = new KafkaCluster(kafkaParam)

	type Err = ArrayBuffer[Throwable]

	/**
		* 若正常则直接返回，若异常则抛出异常
		*
		* @param result 抛出异常
		* @tparam T 正确结果
		* @return
		*/
	def checkErrors[T](result: Either[Err, T]): T = {
		result.fold(
			err => throw new Exception(err.mkString("\n")),
			ok => ok
		)
	}


	private[spark] case class LeaderOffset(host: String, port: Int, offset: Long)

	/**
		* SimpleConsumer直接与broker连接，该类中包含SimpleConsumer的配置信息
		*
		* @param broker
		* @param originalProps
		*/
	private[spark] class SimpleConsumerConfig (broker: String, originalProps: Properties) extends ConsumerConfig(originalProps) {
		val seedBrokers: Array[(String, Int)] = {
			if (broker.trim.isEmpty) throw new Exception("broker地址为空！")
			broker.split(",").map(hostPort => {
				val splitedHostPort = hostPort.split(":")
				if (splitedHostPort.length != 2) {
					throw new Exception("broker地址格式不正确，应为：broker:port[,broker:port....]")
				}
				(splitedHostPort(0), splitedHostPort(1).toInt)
			})
		}
	}

	private[spark] object SimpleConsumerConfig {
		/**
			* 这里主要解析的参数有：bootstrap.servers、metadata.broker.list
			* 将group.id、zookeeper.connect置为空
			*
			* @param kafkaParam 消费者连接broker的参数
			* @return
			*/
		def apply(kafkaParam: Map[String, String]): SimpleConsumerConfig = {
			val brokers = kafkaParam.get("metadata.broker.list")
				.orElse(kafkaParam.get("bootstrap.servers")).getOrElse(throw new Exception("没有指定bootstrap.servers或metadata.broker.list"))

			val props = new Properties()
			kafkaParam.foreach(config => {
				if (config._1 != "metadata.broker.list" && config._1 != "bootstrap.servers") {
					props.put(config._1, config._2);
				}
			})

//			Seq("group.id", "zookeeper.connect").foreach(key => {
//				if (props.containsKey(key)) {
//					props.setProperty(key, "")
//				}
//			})

			new SimpleConsumerConfig(brokers, props)
		}
	}

}
