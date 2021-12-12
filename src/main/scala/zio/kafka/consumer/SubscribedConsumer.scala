package zio.kafka.consumer

import org.apache.kafka.common.TopicPartition
import zio.{ Has, RIO, Task }
import zio.stream.{ Stream, ZStream }
import zio.kafka.serde.Deserializer

class SubscribedConsumer(
  private val underlying: Task[Consumer]
) {

  def partitionedStream[K, V](keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]): Stream[
    Throwable,
    (TopicPartition, Stream[Throwable, CommittableRecord[K, V]])
  ] =
    ZStream.fromEffect(underlying).flatMap(_.partitionedStream(keyDeserializer, valueDeserializer))

  def plainStream[K, V](
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    outputBuffer: Int = 4
  ): Stream[Throwable, CommittableRecord[K, V]] =
    partitionedStream(keyDeserializer, valueDeserializer).flatMapPar(n = Int.MaxValue, outputBuffer = outputBuffer)(
      _._2
    )
}

class SubscribedConsumerFromEnvironment(
  private val underlying: RIO[Has[Consumer], Consumer]
) {

  def partitionedStream[K, V](keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]): ZStream[
    Has[Consumer],
    Throwable,
    (TopicPartition, Stream[Throwable, CommittableRecord[K, V]])
  ] =
    ZStream.fromEffect(underlying).flatMap(_.partitionedStream(keyDeserializer, valueDeserializer))

  def plainStream[K, V](
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    outputBuffer: Int = 4
  ): ZStream[Has[Consumer], Throwable, CommittableRecord[K, V]] =
    partitionedStream(keyDeserializer, valueDeserializer).flatMapPar(n = Int.MaxValue, outputBuffer = outputBuffer)(
      _._2
    )
}
