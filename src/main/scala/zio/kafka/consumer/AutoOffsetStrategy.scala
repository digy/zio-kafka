package zio.kafka.consumer

sealed trait AutoOffsetStrategy { self =>
  def toConfig: String = self match {
    case AutoOffsetStrategy.Earliest => "earliest"
    case AutoOffsetStrategy.Latest   => "latest"
    case AutoOffsetStrategy.None     => "none"
  }
}

object AutoOffsetStrategy {
  case object Earliest extends AutoOffsetStrategy
  case object Latest   extends AutoOffsetStrategy
  case object None     extends AutoOffsetStrategy
}
