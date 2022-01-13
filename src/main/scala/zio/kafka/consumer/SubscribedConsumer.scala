package zio.kafka.consumer

import org.apache.kafka.common.TopicPartition
import zio.{ RIO, Task }
import zio.stream.ZStream
import zio.kafka.serde.Deserializer

class SubscribedConsumer(
  private val underlying: Task[Consumer]
) {

  def partitionedStream[K, V](keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]): ZStream[
    Any,
    Throwable,
    (TopicPartition, ZStream[Any, Throwable, CommittableRecord[K, V]])
  ] =
    ZStream.fromZIO(underlying).flatMap(_.partitionedStream(keyDeserializer, valueDeserializer))

  def plainStream[K, V](
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    outputBuffer: Int = 4
  ): ZStream[Any, Throwable, CommittableRecord[K, V]] =
    partitionedStream(keyDeserializer, valueDeserializer).flatMapPar(n = Int.MaxValue, outputBuffer = outputBuffer)(
      _._2
    )
}

class SubscribedConsumerFromEnvironment(
  private val underlying: RIO[Consumer, Consumer]
) {

  def partitionedStream[K, V](keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]): ZStream[
    Consumer,
    Throwable,
    (TopicPartition, ZStream[Any, Throwable, CommittableRecord[K, V]])
  ] =
    ZStream.fromZIO(underlying).flatMap(_.partitionedStream(keyDeserializer, valueDeserializer))

  def plainStream[K, V](
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    outputBuffer: Int = 4
  ): ZStream[Consumer, Throwable, CommittableRecord[K, V]] =
    partitionedStream(keyDeserializer, valueDeserializer).flatMapPar(n = Int.MaxValue, outputBuffer = outputBuffer)(
      _._2
    )
}
