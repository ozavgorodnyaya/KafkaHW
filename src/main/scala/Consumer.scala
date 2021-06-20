import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters.{IterableHasAsScala, SeqHasAsJava}

object Consumer extends App {

  val properties = new Properties()
  properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1")
  properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
  val consumer = new KafkaConsumer[String, String](properties)

  val topicName = "books"

  try {
    val partitionList = getPartitions(topicName, properties).asJava
    consumer.assign(partitionList)
    consumer.seekToEnd(partitionList)

    for (p <- partitionList.asScala) {
      val lastPos = consumer.position(p)
      println("PartNum: " + p.partition()+ "  lastOffset: " + lastPos )
      consumer.seek(p, lastPos - 5)
    }
    while (true) {
      val recList: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(1))
      for (rec <- recList.asScala) {
        print("PartNum: "+rec.partition() + "  offset: " + rec.offset() + "   Book:")
        println(rec.value())
      }
    }
  } finally {
    consumer.close()
  }

  def getPartitions(topic: String, clusterConfig: Properties) = {
    val topicDesc = Admin.create(clusterConfig)
                         .describeTopics(Collections.singletonList(topic))
                         .values()
                         .get(topicName)
                         .get()
    topicDesc.partitions().asScala
      .map(partitionInfo => new TopicPartition(topicName, partitionInfo.partition()))
      .toList
  }
}