package zio.kafka.consumer

import org.apache.kafka.clients.consumer.{ OffsetAndMetadata, OffsetAndTimestamp }
import org.apache.kafka.common.{ Metric, MetricName, PartitionInfo, TopicPartition }
import zio._
import zio.kafka.serde.Deserializer
import zio.kafka.consumer.diagnostics.Diagnostics
import zio.kafka.consumer.internal.{ ConsumerAccess, Runloop }
import zio.stream._

trait Consumer {

  /**
   * Returns the topic-partitions that this consumer is currently assigned.
   *
   * This is subject to consumer rebalancing, unless using a manual subscription.
   */
  def assignment: Task[Set[TopicPartition]]

  def beginningOffsets(
    partitions: Set[TopicPartition],
    timeout: Duration = Duration.Infinity
  ): Task[Map[TopicPartition, Long]]

  /**
   * Retrieve the last committed offset for the given topic-partitions
   */
  def committed(
    partitions: Set[TopicPartition],
    timeout: Duration = Duration.Infinity
  ): Task[Map[TopicPartition, Option[OffsetAndMetadata]]]

  def endOffsets(
    partitions: Set[TopicPartition],
    timeout: Duration = Duration.Infinity
  ): Task[Map[TopicPartition, Long]]

  def listTopics(timeout: Duration = Duration.Infinity): Task[Map[String, List[PartitionInfo]]]

  /**
   * Create a stream that emits chunks whenever new partitions are assigned to this consumer.
   *
   * The top-level stream will emit chunks whenever the consumer rebalances, unless a manual subscription was made. When
   * rebalancing occurs, new topic-partition streams may be emitted and existing streams may be completed.
   *
   * All streams can be completed by calling [[stopConsumption]].
   */
  def partitionedAssignmentStream[K, V](
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ): Stream[Throwable, Chunk[(TopicPartition, Stream[Throwable, CommittableRecord[K, V]])]]

  /**
   * Create a stream with messages on the subscribed topic-partitions by topic-partition
   *
   * The top-level stream will emit new topic-partition streams for each topic-partition that is assigned to this
   * consumer. This is subject to consumer rebalancing, unless a manual subscription was made. When rebalancing occurs,
   * new topic-partition streams may be emitted and existing streams may be completed.
   *
   * All streams can be completed by calling [[stopConsumption]].
   */
  def partitionedStream[K, V](
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ): Stream[Throwable, (TopicPartition, Stream[Throwable, CommittableRecord[K, V]])]

  /**
   * Create a stream with all messages on the subscribed topic-partitions
   *
   * The stream will emit messages from all topic-partitions interleaved. Per-partition record order is guaranteed, but
   * the topic-partition interleaving is non-deterministic.
   *
   * Up to `outputBuffer` chunks may be buffered in memory by this operator.
   *
   * The stream can be completed by calling [[stopConsumption]].
   */
  def plainStream[K, V](
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    outputBuffer: Int = 4
  ): Stream[Throwable, CommittableRecord[K, V]]

  /**
   * Stops consumption of data, drains buffered records, and ends the attached streams while still serving commit
   * requests.
   */
  def stopConsumption: UIO[Unit]

  /**
   * See [[Consumer.consumeWith]].
   */
  def consumeWith[R1, K, V](
    subscription: Subscription,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    commitRetryPolicy: Schedule[Clock, Any, Any] = Schedule.exponential(1.second) && Schedule.recurs(3)
  )(
    f: (K, V) => URIO[R1, Unit]
  ): ZIO[R1, Throwable, Unit]

  def subscribe(subscripstion: Subscription): Task[Unit]

  def unsubscribe: Task[Unit]

  /**
   * Look up the offsets for the given partitions by timestamp. The returned offset for each partition is the earliest
   * offset whose timestamp is greater than or equal to the given timestamp in the corresponding partition.
   *
   * The consumer does not have to be assigned the partitions. If no messages exist yet for a partition, it will not
   * exist in the returned map.
   */
  def offsetsForTimes(
    timestamps: Map[TopicPartition, Long],
    timeout: Duration = Duration.Infinity
  ): Task[Map[TopicPartition, OffsetAndTimestamp]]

  def partitionsFor(topic: String, timeout: Duration = Duration.Infinity): Task[List[PartitionInfo]]

  def position(partition: TopicPartition, timeout: Duration = Duration.Infinity): Task[Long]

  def subscribeAnd(subscription: Subscription): SubscribedConsumer

  def subscription: Task[Set[String]]

  /**
   * Expose internal consumer metrics
   */
  def metrics: Task[Map[MetricName, Metric]]
}

object Consumer {

  val offsetBatches: ZSink[Any, Nothing, Offset, Nothing, OffsetBatch] =
    ZSink.foldLeft[Offset, OffsetBatch](OffsetBatch.empty)(_ merge _)

  val live: RLayer[Clock with ConsumerSettings with Diagnostics, Consumer] =
    (for {
      settings    <- ZManaged.service[ConsumerSettings]
      diagnostics <- ZManaged.service[Diagnostics]
      consumer    <- make(settings, diagnostics)
    } yield consumer).toLayer

  def make(
    settings: ConsumerSettings,
    diagnostics: Diagnostics = Diagnostics.NoOp
  ): RManaged[Clock, Consumer] =
    for {
      wrapper <- ConsumerAccess.make(settings)
      runloop <- Runloop(
                   wrapper,
                   settings.pollInterval,
                   settings.pollTimeout,
                   diagnostics,
                   settings.offsetRetrieval
                 )
      clock <- ZManaged.service[Clock]
    } yield Live(wrapper, settings, runloop, clock)

  /**
   * Accessor method for [[Consumer.assignment]]
   */
  def assignment: RIO[Consumer, Set[TopicPartition]] =
    ZIO.serviceWithZIO(_.assignment)

  /**
   * Accessor method for [[Consumer.beginningOffsets]]
   */
  def beginningOffsets(
    partitions: Set[TopicPartition],
    timeout: Duration = Duration.Infinity
  ): RIO[Consumer, Map[TopicPartition, Long]] =
    ZIO.serviceWithZIO(_.beginningOffsets(partitions, timeout))

  /**
   * Accessor method for [[Consumer.committed]]
   */
  def committed(
    partitions: Set[TopicPartition],
    timeout: Duration = Duration.Infinity
  ): RIO[Consumer, Map[TopicPartition, Option[OffsetAndMetadata]]] =
    ZIO.serviceWithZIO(_.committed(partitions, timeout))

  /**
   * Accessor method for [[Consumer.endOffsets]]
   */
  def endOffsets(
    partitions: Set[TopicPartition],
    timeout: Duration = Duration.Infinity
  ): RIO[Consumer, Map[TopicPartition, Long]] =
    ZIO.serviceWithZIO(_.endOffsets(partitions, timeout))

  /**
   * Accessor method for [[Consumer.listTopics]]
   */
  def listTopics(
    timeout: Duration = Duration.Infinity
  ): RIO[Consumer, Map[String, List[PartitionInfo]]] =
    ZIO.serviceWithZIO(_.listTopics(timeout))

  /**
   * Accessor method for [[Consumer.partitionedStream]]
   */
  def partitionedStream[K, V](
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ): ZStream[
    Consumer,
    Throwable,
    (TopicPartition, Stream[Throwable, CommittableRecord[K, V]])
  ] =
    ZStream.environmentWithStream(_.get[Consumer].partitionedStream(keyDeserializer, valueDeserializer))

  /**
   * Accessor method for [[Consumer.plainStream]]
   */
  def plainStream[K, V](
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    outputBuffer: Int = 4
  ): ZStream[Consumer, Throwable, CommittableRecord[K, V]] =
    ZStream.environmentWithStream(_.get[Consumer].plainStream(keyDeserializer, valueDeserializer, outputBuffer))

  /**
   * Accessor method for [[Consumer.stopConsumption]]
   */
  def stopConsumption: RIO[Consumer, Unit] =
    ZIO.serviceWithZIO(_.stopConsumption)

  /**
   * Execute an effect for each record and commit the offset after processing
   *
   * This method is the easiest way of processing messages on a Kafka topic.
   *
   * Messages on a single partition are processed sequentially, while the processing of multiple partitions happens in
   * parallel.
   *
   * Offsets are committed after execution of the effect. They are batched when a commit action is in progress to avoid
   * backpressuring the stream. When commits fail due to a
   * org.apache.kafka.clients.consumer.RetriableCommitFailedException they are retried according to commitRetryPolicy
   *
   * The effect should absorb any failures. Failures should be handled by retries or ignoring the error, which will
   * result in the Kafka message being skipped.
   *
   * Messages are processed with 'at least once' consistency: it is not guaranteed that every message that is processed
   * by the effect has a corresponding offset commit before stream termination.
   *
   * Usage example:
   *
   * {{{
   * val settings: ConsumerSettings = ???
   * val subscription = Subscription.Topics(Set("my-kafka-topic"))
   *
   * val consumerIO = Consumer.consumeWith(settings, subscription, Serdes.string, Serdes.string) { case (key, value) =>
   *   // Process the received record here
   *   putStrLn(s"Received record: \${key}: \${value}")
   * }
   * }}}
   *
   * @param settings
   *   Settings for creating a [[Consumer]]
   * @param subscription
   *   Topic subscription parameters
   * @param keyDeserializer
   *   Deserializer for the key of the messages
   * @param valueDeserializer
   *   Deserializer for the value of the messages
   * @param commitRetryPolicy
   *   Retry commits that failed due to a RetriableCommitFailedException according to this schedule
   * @param f
   *   Function that returns the effect to execute for each message. It is passed the key and value
   * @tparam R
   *   Environment for the consuming effect
   * @tparam R1
   *   Environment for the deserializers
   * @tparam K
   *   Type of keys (an implicit `Deserializer` should be in scope)
   * @tparam V
   *   Type of values (an implicit `Deserializer` should be in scope)
   * @return
   *   Effect that completes with a unit value only when interrupted. May fail when the [[Consumer]] fails.
   */
  def consumeWith[R1, K, V](
    settings: ConsumerSettings,
    subscription: Subscription,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    commitRetryPolicy: Schedule[Clock, Any, Any] = Schedule.exponential(1.second) && Schedule.recurs(3)
  )(f: (K, V) => URIO[R1, Unit]): RIO[R1 with Clock, Unit] =
    Consumer
      .make(settings)
      .use(_.consumeWith(subscription, keyDeserializer, valueDeserializer, commitRetryPolicy)(f))

  /**
   * Accessor method for [[Consumer.subscribe]]
   */
  def subscribe(subscription: Subscription): RIO[Consumer, Unit] =
    ZIO.serviceWithZIO(_.subscribe(subscription))

  /**
   * Accessor method for [[Consumer.unsubscribe]]
   */
  def unsubscribe: RIO[Consumer, Unit] =
    ZIO.serviceWithZIO(_.unsubscribe)

  /**
   * Accessor method for [[Consumer.offsetsForTimes]]
   */
  def offsetsForTimes(
    timestamps: Map[TopicPartition, Long],
    timeout: Duration = Duration.Infinity
  ): RIO[Consumer, Map[TopicPartition, OffsetAndTimestamp]] =
    ZIO.serviceWithZIO(_.offsetsForTimes(timestamps, timeout))

  /**
   * Accessor method for [[Consumer.partitionsFor]]
   */
  def partitionsFor(
    topic: String,
    timeout: Duration = Duration.Infinity
  ): RIO[Consumer, List[PartitionInfo]] =
    ZIO.serviceWithZIO(_.partitionsFor(topic, timeout))

  /**
   * Accessor method for [[Consumer.position]]
   */
  def position(
    partition: TopicPartition,
    timeout: Duration = Duration.Infinity
  ): RIO[Consumer, Long] =
    ZIO.serviceWithZIO(_.position(partition, timeout))

  /**
   * Accessor method for [[Consumer.subscribeAnd]]
   */
  def subscribeAnd(
    subscription: Subscription
  ): SubscribedConsumerFromEnvironment =
    new SubscribedConsumerFromEnvironment(
      ZIO.accessM { env =>
        val consumer = env.get[Consumer]
        consumer.subscribe(subscription).as(consumer)
      }
    )

  /**
   * Accessor method for [[Consumer.subscription]]
   */
  def subscription: RIO[Consumer, Set[String]] =
    ZIO.serviceWithZIO(_.subscription)

  /**
   * Accessor method for [[Consumer.metrics]]
   */
  def metrics: RIO[Consumer, Map[MetricName, Metric]] =
    ZIO.serviceWithZIO(_.metrics)

}
