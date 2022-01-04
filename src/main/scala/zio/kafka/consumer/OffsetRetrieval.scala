package zio.kafka.consumer

import org.apache.kafka.common.TopicPartition
import zio.Task

sealed trait OffsetRetrieval

object OffsetRetrieval {
  final case class Auto(reset: AutoOffsetStrategy = AutoOffsetStrategy.Latest)                extends OffsetRetrieval
  final case class Manual(getOffsets: Set[TopicPartition] => Task[Map[TopicPartition, Long]]) extends OffsetRetrieval
}
