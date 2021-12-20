package zio.kafka.consumer

import org.apache.kafka.clients.consumer.{ ConsumerGroupMetadata, RetriableCommitFailedException }
import org.apache.kafka.common.TopicPartition
import zio.{ RIO, Schedule, Task }
import zio.clock.Clock

final case class Offset(
  topicPartition: TopicPartition,
  offset: Long,
  commitHandle: Map[TopicPartition, Long] => Task[Unit],
  consumerGroupMetadata: Option[ConsumerGroupMetadata]
) {
  def commit: Task[Unit] = commitHandle(Map(topicPartition -> offset))
  def batch: OffsetBatch = OffsetBatchImpl(Map(topicPartition -> offset), commitHandle, consumerGroupMetadata)

  /**
   * Attempts to commit and retries according to the given policy when the commit fails with a
   * RetriableCommitFailedException
   */
  def commitOrRetry[R](policy: Schedule[R, Throwable, Any]): RIO[R with Clock, Unit] =
    commitOrRetry(commit, policy)

  private[consumer] def commitOrRetry[R, B](
    commit: Task[Unit],
    policy: Schedule[R, Throwable, B]
  ): RIO[R with Clock, Unit] =
    commit.retry(
      Schedule.recurWhile[Throwable] {
        case _: RetriableCommitFailedException => true
        case _                                 => false
      } && policy
    )
}
