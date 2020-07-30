package eos
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{
  ParallelSourceFunction,
  RichSourceFunction,
  SourceFunction
}

abstract class FixedRateSourceFunction[T](var simulatedRPS: Long = 1000)
    extends RichSourceFunction[T]
    with ParallelSourceFunction[T] {

  def getNextItem(timestampMS: Long): T

  val running = new AtomicBoolean(false)

  var nextTimestamp = -1L

  // If you want 1000/s, then we only need to emit 1 each time
  // But if you wanted 10000/s we would emit 10 each time
  val perEmit: Long = Math.max(1, simulatedRPS / 1000)

  // if you wanted 1000/s, then increment would be 1 (ms)
  // if you wanted 100/s, then incremnt would be 10 (ms)
  // if you wanted 10000/s, then increment would be 1, and we would emit 10 each
  val timeIncrement = Math.max(1, 1000 / simulatedRPS)

  var started: Long = _

  override def open(parameters: Configuration): Unit = {
    running.set(true)
    started = System.currentTimeMillis()
  }

  override def cancel(): Unit = running.set(false)

  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    val startTime = System.currentTimeMillis()
    while (running.get()) {
      if (System.currentTimeMillis() >= nextTimestamp) {
        if (nextTimestamp < 0) {
          nextTimestamp = System.currentTimeMillis()
        }
        (1L to perEmit).foreach(_ => {
          ctx.getCheckpointLock.synchronized {
            ctx.collect(getNextItem(nextTimestamp))
          }
        })

        nextTimestamp = nextTimestamp + timeIncrement

      }
    }
  }

}

object FixedRateSourceFunction {
  def apply[T](simulatedRPS: Long,
               fn: (Long) => T): FixedRateSourceFunction[T] = {
    new FixedRateSourceFunction[T](simulatedRPS) {
      override def getNextItem(timestampMS: Long): T = fn(timestampMS)
    }
  }
}
