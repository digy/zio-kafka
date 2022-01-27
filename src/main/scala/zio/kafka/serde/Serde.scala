package zio.kafka.serde

import org.apache.kafka.common.serialization.{ Serde => KafkaSerde }
import org.apache.kafka.common.header.Headers

import zio.Task

import scala.util.Try
import scala.jdk.CollectionConverters._

/**
 * A serializer and deserializer for values of type T
 *
 * @tparam T
 *   Value type
 */
trait Serde[T] extends Deserializer[T] with Serializer[T] {

  /**
   * Creates a new Serde that uses optional values. Null data will be mapped to None values.
   */
  def asOption(implicit ev: T <:< AnyRef, ev2: Null <:< T): Serde[Option[T]] =
    Serde(super[Deserializer].asOption)(super[Serializer].asOption)

  /**
   * Creates a new Serde that executes its serialization and deserialization functions on the blocking threadpool.
   */
  override def blocking: Serde[T] =
    Serde(super[Deserializer].blocking)(super[Serializer].blocking)

  /**
   * Converts to a Serde of type U with pure transformations
   */
  def inmap[U](f: T => U)(g: U => T): Serde[U] =
    Serde(map(f))(contramap(g))

  /**
   * Convert to a Serde of type U with effectful transformations
   */
  def inmapM[U](f: T => Task[U])(g: U => Task[T]): Serde[U] =
    Serde(mapM(f))(contramapZIO(g))
}

object Serde extends Serdes {

  /**
   * Create a Serde from a deserializer and serializer function
   *
   * The (de)serializer functions can returned a failure ZIO with a Throwable to indicate (de)serialization failure
   */
  def apply[T](
    deser: (String, Headers, Array[Byte]) => Task[T]
  )(ser: (String, Headers, T) => Task[Array[Byte]]): Serde[T] =
    new Serde[T] {
      override def serialize(topic: String, headers: Headers, value: T): Task[Array[Byte]] =
        ser(topic, headers, value)
      override def deserialize(topic: String, headers: Headers, data: Array[Byte]): Task[T] =
        deser(topic, headers, data)
    }

  /**
   * Create a Serde from a deserializer and serializer function
   */
  def apply[T](deser: Deserializer[T])(ser: Serializer[T]): Serde[T] = new Serde[T] {
    override def serialize(topic: String, headers: Headers, value: T): Task[Array[Byte]] =
      ser.serialize(topic, headers, value)
    override def deserialize(topic: String, headers: Headers, data: Array[Byte]): Task[T] =
      deser.deserialize(topic, headers, data)
  }

  /**
   * Create a Serde from a Kafka Serde
   */
  def fromKafkaSerde[T](serde: KafkaSerde[T], props: Map[String, AnyRef], isKey: Boolean) =
    Task(serde.configure(props.asJava, isKey))
      .as(
        new Serde[T] {
          val serializer   = serde.serializer()
          val deserializer = serde.deserializer()

          override def deserialize(topic: String, headers: Headers, data: Array[Byte]): Task[T] =
            Task(deserializer.deserialize(topic, headers, data))

          override def serialize(topic: String, headers: Headers, value: T): Task[Array[Byte]] =
            Task(serializer.serialize(topic, headers, value))
        }
      )

  implicit def deserializerWithError[R, T](implicit deser: Deserializer[T]): Deserializer[Try[T]] =
    deser.asTry
}
