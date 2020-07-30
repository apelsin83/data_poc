package producer

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.{
  PredefinedOptions,
  RocksDBOptions,
  RocksDBStateBackend
}
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{
  FlinkKafkaConsumer,
  FlinkKafkaProducer
}
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG
import org.apache.flink.streaming.api.scala._

object Producer {
  private def getRocksConfigurationOptions: Configuration = {
    val configuration: Configuration = new Configuration()
    configuration.setString(
      RocksDBOptions.TIMER_SERVICE_FACTORY.toString,
      RocksDBStateBackend.PriorityQueueStateType.ROCKSDB.toString
    )

    configuration
  }

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    //Checkpoints
    val checkpoint: Long = Time.seconds(10).toMilliseconds
    env.enableCheckpointing(checkpoint)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(checkpoint)

    //State backend rocksdb
    var backend =
      new RocksDBStateBackend(s"file:///opt/flink/checkpoints", false)
    backend.setPredefinedOptions(
      PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM)
    backend = backend.configure(getRocksConfigurationOptions,
                                Thread.currentThread().getContextClassLoader)
    env.setStateBackend(backend.asInstanceOf[StateBackend])

    val sourceProperties = new Properties
    sourceProperties.setProperty("bootstrap.servers", "localhost:9092")
    sourceProperties.setProperty("security.protocol", "PLAINTEXT")
    sourceProperties.setProperty("group.id", "ddos")
    sourceProperties.setProperty("auto.offset.reset", "latest")

    val sinkProperties = new Properties
    sinkProperties.setProperty("bootstrap.servers", "localhost:9092")
    sinkProperties.setProperty("security.protocol", "PLAINTEXT")
    sinkProperties.setProperty("batch.size", "512000")
    sinkProperties.setProperty("max.request.size", "1048576")
    sinkProperties.setProperty("linger.ms", "200")
    sinkProperties.setProperty("compression.type", "lz4")
    sinkProperties.setProperty("acks", "all")
    sinkProperties.setProperty("transaction.timeout.ms", "360000")
    sinkProperties.setProperty(ENABLE_IDEMPOTENCE_CONFIG, "true")

    val source =
      new FlinkKafkaConsumer("ddos", new SimpleStringSchema(), sourceProperties)

    val sink = new FlinkKafkaProducer[String](
      "etld", // target topic
      new KeyedSerializationSchemaWrapper(new SimpleStringSchema()),
      sinkProperties,
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE)

    env
      .addSource(source)
      .uid("Source")
      .name("Source")
      .addSink(sink)
      .uid("Etl")
      .name("Etl")
    env.execute("etl")
  }
}
