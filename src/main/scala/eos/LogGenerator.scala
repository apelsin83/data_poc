package eos

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper

object LogGenerator {

    def main(args: Array[String]): Unit = {

      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

      val properties = new Properties
      properties.setProperty("bootstrap.servers", "localhost:9092")
      properties.setProperty("security.protocol", "PLAINTEXT")
      properties.setProperty("batch.size", "512000")
      properties.setProperty("max.request.size", "1048576")
      properties.setProperty("linger.ms", "200")
      properties.setProperty("compression.type", "lz4")
      properties.setProperty("acks", "all")
//      properties.setProperty("transaction.timeout.ms", "localhost:9092")
//      properties.setProperty(ENABLE_IDEMPOTENCE_CONFIG, "true"

      val sourceRPS = 1
      val sourceParallelism = 1
      val sink = new FlinkKafkaProducer[String](
        "ddos",                  // target topic
        new KeyedSerializationSchemaWrapper(new SimpleStringSchema()),
        properties,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)

      val source = FixedRateSourceFunction(
        sourceRPS / sourceParallelism,
        (timestamp: Long) => timestamp.toString
      )

      env
        .addSource(source)
        .setParallelism(sourceParallelism)
        .addSink(sink)
      env.execute("Waf Log Generator")
    }
}
