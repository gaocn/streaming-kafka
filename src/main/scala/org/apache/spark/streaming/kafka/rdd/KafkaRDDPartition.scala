package org.apache.spark.streaming.kafka.rdd

import org.apache.spark.Partition


/**
	* Kakfa RDD分区
 *
	* @param index Kafka RDD分区标识
	* @param topic 对应kafka主题名
	* @param partition 对应kafka主题分区标识
	* @param fromOffset 对应kafka分区数据的起始位置(包含)
	* @param untilOffset 对应kafka分区数据的结束位置(不包含)
	* @param host 对应kafka分区所在主机名
	* @param port 对应kafka分区所在主机名端口名
	*/
private[kafka] class KafkaRDDPartition(
		val index: Int,
		val topic: String,
		val partition: Int,
		val fromOffset: Long,
		val untilOffset: Long,
		val host: String,
		val port: Int
	) extends Partition{

	/**
		* 当前分区数据条数
		*/
	def count() = untilOffset - fromOffset
}
