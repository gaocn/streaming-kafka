package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.KafkaCluster.{Err, LeaderOffset}
import org.junit.{Before, Test}

class TestKakfaCluster {
	var cluster:KafkaCluster = null

	@Before
	def init = {
		val kafkaParams = Map[String, String](
			"bootstrap.servers"->"10.230.135.124:9093,10.230.135.125:9093",
			"zookeeper.connect"->"10.230.135.124:2181,10.230.135.125:2181",
			"group.id"->"kafka-cluster-test"
		)
		cluster =  KafkaCluster(kafkaParams)
	}

	@Test
	def testFindLeader() = {
		val topic = "Log_MCSS_LOG"
		val partition = 1
		val resp: Either[Err, (String, Int)] = cluster.findLeader(topic, partition)
		println(resp)
	}

	@Test
	def testGetPartitionMetadata(): Unit = {
		val topics = Set(
			"Log_MCSS_LOG",
			"Log_BJS_Log"
		)
		println(cluster.getPartitionMetadata(topics))
	}

	@Test
	def testGetPartitions(): Unit = {
		val topics = Set(
			"Log_MCSS_LOG",
			"Log_BJS_Log"
		)
		println(cluster.getPartitions(topics))
	}

	@Test
	def testGetLeaderOffset(): Unit = {
		val topic = "Log_BJS_Log"
		val tp = new TopicAndPartition(topic, 0)

		println(cluster.getEarliestLeaderOffset(Set(tp)))
		println(cluster.getLatestLeadereOffset(Set(tp)))

		var untilOffsets:Map[TopicAndPartition, LeaderOffset]= cluster.getLeaderOffset(Set(tp), System.currentTimeMillis()).fold(
			err=> null.asInstanceOf[Map[TopicAndPartition, LeaderOffset]],
			tpLOMap => tpLOMap)

		println(untilOffsets)
	}

	@Test
	def testGetLatestLeadereOffset(): Unit = {
		val topics = Set(
			"Log_MCSS_LOG",
			"Log_BJS_Log"
		)
		cluster.getPartitions(topics).fold(
			err => println(err),
			tp =>
				println(cluster.getLatestLeadereOffset(tp))
		)
	}

	@Test
	def testGetEarliestLeaderOffset(): Unit = {
		val topics = Set(
			"Log_MCSS_LOG",
			"Log_BJS_Log"
		)
		cluster.getPartitions(topics).fold(
			err => println(err),
			tp =>
				println(cluster.getEarliestLeaderOffset(tp))
		)
	}

	@Test
	def testGetConsumerOffsets(): Unit = {
		val topics = Set(
			"Log_MBank_LOG",
			"Log_BJS_Log"
		)
		val topicAndPartitions = topics.map(t => new TopicAndPartition(t, 0))
		println(cluster.getConsumerOffsets("elog4-logstash2",topicAndPartitions))
	}
}
