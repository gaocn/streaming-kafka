package org.apache.spark.streaming.kafka.domain

import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.domain.OffsetRange.OffsetRangeTuple

/**
	* 返回对象内部[[OffsetRange]]集合
	*
	* 当使用Direct Kafka Stream获取RDD时，可以返回RDD分区所有包含的
	* 对应Kafka分区中能够读取的偏移量范围及数据所在位置。
	*
	* 例如：
	* {{{
	*   val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
	* }}}
	*
	*/
trait HasOffsetRanges {
	def offsetRanges: Array[OffsetRange]
}

/**
	* 某个主题的一个分区的偏移量范围（元数据)，以便在RDD中进行偏移量管理
	*
	* @param topic       主题名称
	* @param partition   主题中的某个分区ID
	* @param fromOffset  偏移量起始位置(包含)
	* @param untilOffset 偏移量结束位置(不包含)
	*/
final class OffsetRange private(
																 val topic: String,
																 val partition: Int,
																 val fromOffset: Long,
																 val untilOffset: Long) extends Serializable {

	/**
		* 转换为Kafka的TopicAndPartition
		*
		* @return TopicAndPartition
		*/
	def topicAndPartition(): TopicAndPartition = TopicAndPartition(topic, partition)

	/**
		* 当前[[OffsetRange]]中记录条数
		*
		* @return 记录条数
		*/
	def count(): Long = untilOffset - fromOffset

	/**
		* 避免在checkpoint恢复时报`ClassNotFoundException`异常
		*/
	//TODO 未验证
	private[streaming] def toTuple: OffsetRangeTuple = (topic, partition, fromOffset, untilOffset)


	override def equals(other: Any): Boolean = other match {
		case that: OffsetRange =>
			topic == that.topic &&
				partition == that.partition &&
				fromOffset == that.fromOffset &&
				untilOffset == that.untilOffset
		case _ => false
	}

	override def hashCode(): Int = {
		val state = Seq(topic, partition, fromOffset, untilOffset)
		state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
	}

	override def toString = s"OffsetRange($topic, $partition, $fromOffset, $untilOffset)"
}

/**
	* 伴生对象，用于创建[[OffsetRange]]
	*/
object OffsetRange {
	/**
		* 避免在checkpoint恢复时报`ClassNotFoundException`异常
		*/
	private[kafka] type OffsetRangeTuple = (String, Int, Long, Long)

	private[kafka] def apply(offsetRangeTuple: OffsetRangeTuple) = new OffsetRange(offsetRangeTuple._1, offsetRangeTuple._2, offsetRangeTuple._3, offsetRangeTuple._4)

	def create(topic: String, partition: Int, fromOffset: Long, untilOffset: Long): OffsetRange = new OffsetRange(topic, partition, fromOffset, untilOffset)

	def create(topicPartition: TopicAndPartition, fromOffset: Long, untilOffset: Long): OffsetRange = new OffsetRange(topicPartition.topic, topicPartition.partition, fromOffset, untilOffset)

	def apply(topic: String, partition: Int, fromOffset: Long, untilOffset: Long): OffsetRange = new OffsetRange(topic, partition, fromOffset, untilOffset)


	def apply(topicPartition: TopicAndPartition, fromOffset: Long, untilOffset: Long): OffsetRange = new OffsetRange(topicPartition.topic, topicPartition.partition, fromOffset, untilOffset)

	def unapply(arg: OffsetRange): Option[(String, Int, Long, Long)] = if (arg == null) None else Some((arg.topic, arg.partition, arg.fromOffset, arg.untilOffset))
}
