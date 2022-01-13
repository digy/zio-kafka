package zio.kafka.serde

import zio.{ Task, ZIO }
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.{ Serializer => KafkaSerializer }

import scala.jdk.CollectionConverters._

/**
 * Serializer from values of some type T to a byte array
 *
 * @tparam T
 */
trait Serializer[-T] {
  def serialize(topic: String, headers: Headers, value: T): Task[Array[Byte]]

  /**
   * Create a serializer for a type U based on the serializer for type T and a mapping function
   */
  def contramap[U](f: U => T): Serializer[U] =
    Serializer((topic, headers, u) => serialize(topic, headers, f(u)))

  /**
   * Create a serializer for a type U based on the serializer for type T and an effectful mapping function
   */
  def contramapM[U](f: U => Task[T]): Serializer[U] =
    Serializer((topic, headers, u) => f(u).flatMap(serialize(topic, headers, _)))

  /**
   * Returns a new serializer that executes its serialization function on the blocking threadpool.
   */
  def blocking: Serializer[T] =
    Serializer((topic, headers, t) => ZIO.attemptBlocking(serialize(topic, headers, t)))

  /**
   * Returns a new serializer that handles optional values and serializes them as nulls.
   */
  def asOption[U <: T](implicit ev: Null <:< T): Serializer[Option[U]] =
    contramap(_.orNull)
}

object Serializer extends Serdes {

  /**
   * Create a serializer from a function
   */
  def apply[R, T](ser: (String, Headers, T) => Task[Array[Byte]]): Serializer[T] =
    new Serializer[T] {
      override def serialize(topic: String, headers: Headers, value: T): Task[Array[Byte]] =
        ser(topic, headers, value)
    }

  /**
   * Create a Serializer from a Kafka Serializer
   */
  def fromKafkaSerializer[T](
    serializer: KafkaSerializer[T],
    props: Map[String, AnyRef],
    isKey: Boolean
  ): Task[Serializer[T]] =
    Task(serializer.configure(props.asJava, isKey))
      .as(
        new Serializer[T] {
          override def serialize(topic: String, headers: Headers, value: T): Task[Array[Byte]] =
            Task(serializer.serialize(topic, headers, value))
        }
      )

}
