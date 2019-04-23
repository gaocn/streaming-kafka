package org.apache.spark.streaming.kafka.domain

/**
	* 代表Kafka Broker：(IP，Port)
	*/
final class Broker private(val host: String, val port: Int) extends Serializable {
	override def equals(obj: scala.Any): Boolean = obj match {
		case that: Broker  =>
			this.host == that.host && this.port == that.port
		case _ => false
	}

	override def hashCode(): Int = {
		41 * (host.hashCode + 41) + port
	}

	override def toString: String = {
		s"Broker(${host}, ${port})"
	}
}


object Broker {
	def apply( host: String,port: Int): Broker = new Broker(host,port)
	def create(host: String, port: Int): Broker = new Broker(host, port)

	def unapply(arg: Broker): Option[(String, Int)] = if(arg == null) {
		None
	} else {
		Some((arg.host, arg.port))
	}
}
