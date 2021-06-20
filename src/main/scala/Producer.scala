import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.Properties

import io.circe._
import io.circe.generic.auto._
import org.apache.commons.csv.{CSVFormat, CSVParser}

import scala.jdk.CollectionConverters.IterableHasAsScala
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Try

object Producer extends App {
  //val bufferedSource = io.Source.fromFile("src/resources/test.csv")
  //val list = line.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)").map(_.trim)

  val filePath = Paths.get("src/resources/bestsellers_with_categories-1801-9dc31f.csv")
  val data =CSVParser.parse(filePath, StandardCharsets.UTF_8,
    CSVFormat.RFC4180.withHeader("Name","Author","UserRating","Reviews","Price","Year","Genre")
      .withSkipHeaderRecord)

  case class Book(
                   Name:	String,
                   Author:	String,
                   UserRating:  Double,
                   Reviews: Int,
                   Price: Int,
                   Year: Int,
                   Genre: String
    )

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:29092")
  val producer = new KafkaProducer(props, new StringSerializer, new StringSerializer)

  try{
  for (record <- data.asScala) {
    val oneBook = Book(record.get("Name"),
                       record.get("Author"),
                       record.get("UserRating").toDouble,
                       record.get("Reviews").toInt,
                       record.get("Price").toInt,
                       record.get("Year").toInt,
                       record.get("Genre"))

    val jsonRes: Json = Encoder[Book].apply(oneBook)
    //println(jsonRes.noSpaces)
    //println()

    val oneRecord = new ProducerRecord[String, String]("books", null, jsonRes.noSpaces)
    producer.send(oneRecord)
  }
  producer.flush()
  } finally {
    producer.close()
  }
}


