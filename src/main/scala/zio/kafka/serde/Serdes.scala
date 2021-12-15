package zio.kafka.serde

import java.nio.ByteBuffer
import java.util.UUID

import org.apache.kafka.common.serialization.{ Serde => KafkaSerde, Serdes => KafkaSerdes }
import org.apache.kafka.common.header.Headers
import zio.{ RIO, Task }

private[zio] trait Serdes {
  lazy val long: Serde[Long]     = convertPrimitiveSerde(KafkaSerdes.Long()).inmap(Long2long)(long2Long)
  lazy val int: Serde[Int]       = convertPrimitiveSerde(KafkaSerdes.Integer()).inmap(Integer2int)(int2Integer)
  lazy val short: Serde[Short]   = convertPrimitiveSerde(KafkaSerdes.Short()).inmap(Short2short)(short2Short)
  lazy val float: Serde[Float]   = convertPrimitiveSerde(KafkaSerdes.Float()).inmap(Float2float)(float2Float)
  lazy val double: Serde[Double] = convertPrimitiveSerde(KafkaSerdes.Double()).inmap(Double2double)(double2Double)
  lazy val string: Serde[String] = convertPrimitiveSerde(KafkaSerdes.String())
  lazy val byteArray: Serde[Array[Byte]] = convertPrimitiveSerde(KafkaSerdes.ByteArray())
  lazy val byteBuffer: Serde[ByteBuffer] = convertPrimitiveSerde(KafkaSerdes.ByteBuffer())
  lazy val uuid: Serde[UUID]             = convertPrimitiveSerde(KafkaSerdes.UUID())

  private[this] def convertPrimitiveSerde[T](serde: KafkaSerde[T]): Serde[T] =
    new Serde[T] {
      val serializer   = serde.serializer()
      val deserializer = serde.deserializer()

      override def deserialize(topic: String, headers: Headers, data: Array[Byte]): RIO[Any, T] =
        Task(deserializer.deserialize(topic, headers, data))

      override def serialize(topic: String, headers: Headers, value: T): RIO[Any, Array[Byte]] =
        Task(serializer.serialize(topic, headers, value))
    }
}
