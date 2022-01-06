package zio.kafka.serde

import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.{ Deserializer => KafkaDeserializer }
import zio.{ Has, Task, ZIO }
import zio.blocking.{ blocking => zioBlocking, Blocking }

import scala.util.{ Failure, Success, Try }
import scala.jdk.CollectionConverters._

/**
 * Deserializer from byte array to a value of some type T
 *
 * @tparam T
 *   Value type
 */
trait Deserializer[+T] {
  def deserialize(topic: String, headers: Headers, data: Array[Byte]): Task[T]

  /**
   * Returns a new deserializer that executes its deserialization function on the blocking threadpool.
   */
  def blocking: Deserializer[T] =
    Deserializer((topic, headers, data) =>
      zioBlocking(deserialize(topic, headers, data)).provide(Has(Blocking.Service.live))
    )
  //Deserializer((topic, headers, data) => ZIO.blocking(deserialize(topic, headers, data)))

  /**
   * Create a deserializer for a type U based on the deserializer for type T and a mapping function
   */
  def map[U](f: T => U): Deserializer[U] = Deserializer(deserialize(_, _, _).map(f))

  /**
   * Create a deserializer for a type U based on the deserializer for type T and an effectful mapping function
   */
  def mapM[U](f: T => Task[U]): Deserializer[U] = Deserializer(deserialize(_, _, _).flatMap(f))

  /**
   * When this serializer fails, attempt to deserialize with the alternative
   *
   * If both deserializers fail, the error will be the last deserializer's exception.
   */
  def orElse[U >: T](alternative: Deserializer[U]): Deserializer[U] =
    Deserializer { (topic, headers, data) =>
      deserialize(topic, headers, data) orElse alternative.deserialize(topic, headers, data)
    }

  /**
   * Serde that handles deserialization failures by returning a Task
   *
   * This is useful for explicitly handling deserialization failures.
   */
  def asTry: Deserializer[Try[T]] =
    Deserializer(deserialize(_, _, _).fold(e => Failure(e), v => Success(v)))

  /**
   * Returns a new deserializer that deserializes values as Option values, mapping null data to None values.
   */
  def asOption(implicit ev: T <:< AnyRef): Deserializer[Option[T]] = {
    val _ = ev
    Deserializer((topic, headers, data) => ZIO.foreach(Option(data))(deserialize(topic, headers, _)))
  }
}

object Deserializer extends Serdes {

  /**
   * Create a deserializer from a function
   */
  def apply[T](deser: (String, Headers, Array[Byte]) => Task[T]): Deserializer[T] = new Deserializer[T] {
    override def deserialize(topic: String, headers: Headers, data: Array[Byte]): Task[T] =
      deser(topic, headers, data)
  }

  /**
   * Create a Deserializer from a Kafka Deserializer
   */
  def fromKafkaDeserializer[T](
    deserializer: KafkaDeserializer[T],
    props: Map[String, AnyRef],
    isKey: Boolean
  ): Task[Deserializer[T]] =
    Task(deserializer.configure(props.asJava, isKey))
      .as(
        new Deserializer[T] {
          override def deserialize(topic: String, headers: Headers, data: Array[Byte]): Task[T] =
            Task(deserializer.deserialize(topic, headers, data))
        }
      )
}
