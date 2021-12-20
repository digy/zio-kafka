package zio.kafka.consumer

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.common.TopicPartition
import zio.Task
import zio.{ RIO, Schedule }
import zio.clock.Clock

object OffsetBatch {
  val empty: OffsetBatch = OffsetBatch(
    offsets = Map(),
    commitHandle = Map(),
    consumerGroupMetadata = None
  )

  def apply(offsets: Iterable[Offset]): OffsetBatch = offsets.foldLeft(empty)(_ merge _)
}

final case class OffsetBatch(
  offsets: Map[TopicPartition, Long],
  commitHandle: Map[TopicPartition, Long] => Task[Unit],
  consumerGroupMetadata: Option[ConsumerGroupMetadata]
) {
  def commit: Task[Unit] = commitHandle(offsets)

  def merge(offset: Offset): OffsetBatch =
    copy(
      offsets = offsets + (offset.topicPartition -> (offsets
        .getOrElse(offset.topicPartition, -1L) max offset.offset))
    )

  def merge(otherOffsets: OffsetBatch): OffsetBatch = {
    val newOffsets = Map.newBuilder[TopicPartition, Long]
    newOffsets ++= offsets
    otherOffsets.offsets.foreach { case (tp, offset) =>
      val existing = offsets.getOrElse(tp, -1L)
      if (existing < offset)
        newOffsets += tp -> offset
    }

    copy(offsets = newOffsets.result())
  }

  def commitOrRetry[R](policy: Schedule[R, Throwable, Any]): RIO[R with Clock, Unit] =
    Offset.commitOrRetry(commit, policy)
}
