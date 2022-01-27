package zio.kafka

import org.apache.kafka.common.internals.KafkaFutureImpl
import zio._
import zio.kafka.admin._
import zio.test._
import zio.test.TestAspect.flaky
import zio.test.Assertion._

object KafkaFutureSpec extends DefaultRunnableSpec {
  override def spec =
    suite("kafka future conversion")(
      test("completes successfully") {
        withKafkaFuture.use { f: KafkaFutureImpl[Boolean] =>
          for {
            fiber  <- AdminClient.fromKafkaFuture(ZIO.succeed(f)).fork
            _      <- ZIO.succeed(f.complete(true))
            result <- fiber.await
          } yield assert(result)(equalTo(Exit.succeed(true))) &&
            assert(f.isDone)(equalTo(true))
        }
      },
      test("completes with failure") {
        withKafkaFuture.use { f =>
          val t = new RuntimeException("failure")
          for {
            fiber  <- AdminClient.fromKafkaFuture(ZIO.succeed(f)).fork
            _      <- ZIO.succeed(f.completeExceptionally(t))
            result <- fiber.await
          } yield assert(result)(equalTo(Exit.fail(t))) &&
            assert(f.isDone)(equalTo(true))
        }
      },
      test("future is cancelled") {
        withKafkaFuture.use { f =>
          for {
            fiber  <- AdminClient.fromKafkaFuture(ZIO.succeed(f)).fork
            _      <- ZIO.succeed(f.cancel(true))
            result <- fiber.await
          } yield assert(result.isInterrupted)(equalTo(true)) &&
            assert(f.isCancelled)(equalTo(true)) &&
            assert(f.isDone)(equalTo(true))
        }
      },
      test("interrupted") {
        withKafkaFuture.use { f =>
          for {
            latch  <- Promise.make[Nothing, Unit]
            fiber  <- AdminClient.fromKafkaFuture(latch.succeed(()) *> ZIO.succeed(f)).fork
            _      <- latch.await
            result <- fiber.interrupt
          } yield assert(result.isInterrupted)(equalTo(true)) &&
            assert(f.isCancelled)(equalTo(true)) &&
            assert(f.isDone)(equalTo(true))
        }
      } @@ flaky
    )

  def withKafkaFuture =
    ZIO.succeed(new KafkaFutureImpl[Boolean]).toManagedWith { f =>
      ZIO.succeed {
        f.completeExceptionally(new RuntimeException("Kafka future was not completed"))
      }
    }
}
