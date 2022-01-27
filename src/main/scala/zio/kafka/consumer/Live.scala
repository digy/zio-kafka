package zio.kafka.consumer

import org.apache.kafka.clients.consumer.{ OffsetAndMetadata, OffsetAndTimestamp }
import org.apache.kafka.common.{ Metric, MetricName, PartitionInfo, TopicPartition }
import zio._
import zio.kafka.consumer.Consumer.offsetBatches
import zio.kafka.serde.Deserializer
import zio.kafka.consumer.internal.{ ConsumerAccess, Runloop }
import zio.stream._

import scala.jdk.CollectionConverters._

private final case class Live(
  private val consumer: ConsumerAccess,
  private val settings: ConsumerSettings,
  private val runloop: Runloop,
  private val clock: Clock
) extends Consumer {

  override def assignment: Task[Set[TopicPartition]] =
    consumer.withConsumer(_.assignment().asScala.toSet)

  override def beginningOffsets(
    partitions: Set[TopicPartition],
    timeout: Duration = Duration.Infinity
  ): Task[Map[TopicPartition, Long]] =
    consumer.withConsumer(
      _.beginningOffsets(partitions.asJava, timeout.asJava).asScala.map { case (tp, l) =>
        tp -> l.longValue()
      }.toMap
    )

  override def committed(
    partitions: Set[TopicPartition],
    timeout: Duration = Duration.Infinity
  ): Task[Map[TopicPartition, Option[OffsetAndMetadata]]] =
    consumer.withConsumer(
      _.committed(partitions.asJava, timeout.asJava).asScala.map { case (k, v) => k -> Option(v) }.toMap
    )

  override def endOffsets(
    partitions: Set[TopicPartition],
    timeout: Duration = Duration.Infinity
  ): Task[Map[TopicPartition, Long]] =
    consumer.withConsumer { eo =>
      val offs = eo.endOffsets(partitions.asJava, timeout.asJava)
      offs.asScala.map { case (k, v) => k -> v.longValue() }.toMap
    }

  /**
   * Stops consumption of data, drains buffered records, and ends the attached streams while still serving commit
   * requests.
   */
  override def stopConsumption: UIO[Unit] =
    runloop.gracefulShutdown

  override def listTopics(timeout: Duration = Duration.Infinity): Task[Map[String, List[PartitionInfo]]] =
    consumer.withConsumer(_.listTopics(timeout.asJava).asScala.map { case (k, v) => k -> v.asScala.toList }.toMap)

  override def offsetsForTimes(
    timestamps: Map[TopicPartition, Long],
    timeout: Duration = Duration.Infinity
  ): Task[Map[TopicPartition, OffsetAndTimestamp]] =
    consumer.withConsumer(
      _.offsetsForTimes(timestamps.map { case (k, v) => k -> Long.box(v) }.toMap.asJava, timeout.asJava).asScala.toMap
        // If a partition doesn't exist yet, the map will have 'null' as entry.
        // It's more idiomatic scala to then simply not have that map entry.
        .filter(_._2 != null)
    )

  override def partitionedAssignmentStream[K, V](
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ): Stream[Throwable, Chunk[(TopicPartition, Stream[Throwable, CommittableRecord[K, V]])]] =
    ZStream
      .fromQueue(runloop.partitions)
      .map(_.exit)
      .flattenExitOption
      .map {
        _.map { case (tp, partition) =>
          val partitionStream =
            if (settings.perPartitionChunkPrefetch <= 0) partition
            else partition.buffer(settings.perPartitionChunkPrefetch)

          tp -> partitionStream.mapChunksZIO(_.mapZIO(_.deserializeWith(keyDeserializer, valueDeserializer)))
        }
      }

  override def partitionedStream[K, V](
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ): Stream[
    Throwable,
    (TopicPartition, Stream[Throwable, CommittableRecord[K, V]])
  ] = partitionedAssignmentStream(keyDeserializer, valueDeserializer).flattenChunks

  override def partitionsFor(
    topic: String,
    timeout: Duration = Duration.Infinity
  ): Task[List[PartitionInfo]] =
    consumer.withConsumer { c =>
      val partitions = c.partitionsFor(topic, timeout.asJava)
      if (partitions eq null) List.empty else partitions.asScala.toList
    }

  override def position(partition: TopicPartition, timeout: Duration = Duration.Infinity): Task[Long] =
    consumer.withConsumer(_.position(partition, timeout.asJava))

  override def plainStream[K, V](
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    outputBuffer: Int
  ): Stream[Throwable, CommittableRecord[K, V]] =
    partitionedStream(keyDeserializer, valueDeserializer).flatMapPar(n = Int.MaxValue, bufferSize = outputBuffer)(
      _._2
    )

  override def subscribeAnd(subscription: Subscription): SubscribedConsumer =
    new SubscribedConsumer(subscribe(subscription).as(this))

  override def subscription: Task[Set[String]] =
    consumer.withConsumer(_.subscription().asScala.toSet)

  override def consumeWith[R1, K, V](
    subscription: Subscription,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    commitRetryPolicy: Schedule[Clock, Any, Any] = Schedule.exponential(1.second) && Schedule.recurs(3)
  )(
    f: (K, V) => URIO[R1, Unit]
  ): ZIO[R1, Throwable, Unit] =
    for {
      r <- ZIO.environment[R1]
      _ <- ZStream
             .fromZIO(subscribe(subscription))
             .flatMap { _ =>
               partitionedStream(keyDeserializer, valueDeserializer)
                 .flatMapPar(Int.MaxValue, bufferSize = settings.perPartitionChunkPrefetch) {
                   case (_, partitionStream) =>
                     partitionStream.mapChunksZIO(_.mapZIO { case CommittableRecord(record, offset) =>
                       f(record.key(), record.value()).as(offset)
                     })
                 }
             }
             .provideEnvironment(r)
             .aggregateAsync(offsetBatches)
             .mapZIO(_.commitOrRetry(commitRetryPolicy))
             .provideEnvironment(ZEnvironment(clock))
             .runDrain
    } yield ()

  override def subscribe(subscription: Subscription): Task[Unit] =
    ZIO.runtime[Any].flatMap { runtime =>
      consumer.withConsumerM { c =>
        subscription match {
          case Subscription.Pattern(pattern) =>
            ZIO(c.subscribe(pattern.pattern, runloop.rebalanceListener.toKafka(runtime)))
          case Subscription.Topics(topics) =>
            ZIO(c.subscribe(topics.asJava, runloop.rebalanceListener.toKafka(runtime)))

          // For manual subscriptions we have to do some manual work before starting the run loop
          case Subscription.Manual(topicPartitions) =>
            ZIO(c.assign(topicPartitions.asJava)) *>
              ZIO.foreach(topicPartitions)(runloop.newPartitionStream).flatMap { partitionStreams =>
                runloop.partitions.offer(
                  Take.chunk(
                    Chunk.fromIterable(partitionStreams.map { case (tp, _, stream) =>
                      tp -> stream
                    })
                  )
                )
              } *> {
                settings.offsetRetrieval match {
                  case OffsetRetrieval.Manual(getOffsets) =>
                    getOffsets(topicPartitions).flatMap { offsets =>
                      ZIO.foreachDiscard(offsets) { case (tp, offset) => ZIO(c.seek(tp, offset)) }
                    }
                  case OffsetRetrieval.Auto(_) => ZIO.unit
                }
              }
        }
      }
    }

  override def unsubscribe: Task[Unit] =
    consumer.withConsumer(_.unsubscribe())

  override def metrics: Task[Map[MetricName, Metric]] =
    consumer.withConsumer(_.metrics().asScala.toMap)
}
