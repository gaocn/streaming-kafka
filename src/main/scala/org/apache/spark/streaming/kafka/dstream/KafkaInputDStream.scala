package org.apache.spark.streaming.kafka.dstream

import kafka.serializer.Decoder
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.receiver.{KafkaReceiver, ReliableKafkaReceiver}
import org.apache.spark.streaming.receiver.Receiver

import scala.reflect.ClassTag

class KafkaInputDStream[
K: ClassTag,
V: ClassTag,
U <: Decoder[_] : ClassTag,
T <: Decoder[_] : ClassTag
](
	 ssc: StreamingContext,
	 kafkaParams: Map[String, String],
	 topics: Map[String, Int],
	 useReliableReceiver: Boolean,
	 storageLevel: StorageLevel
 ) extends ReceiverInputDStream[(K, V)](ssc) with Logging{
	override def getReceiver(): Receiver[(K, V)] = {
		if(!useReliableReceiver)  {
			new  KafkaReceiver[K,V,U,T](kafkaParams, topics, storageLevel)
		} else {
			new  ReliableKafkaReceiver[K,V,U,T](kafkaParams, topics, storageLevel)
		}
	}
}
