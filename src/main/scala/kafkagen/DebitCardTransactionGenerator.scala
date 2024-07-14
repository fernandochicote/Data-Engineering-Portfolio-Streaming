package kafkagen

import java.util.{Properties, UUID}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.sql.Timestamp
import scala.util.{Try, Success, Failure}
import scala.util.Random

object DebitCardTransactionGenerator {

  private val props: Properties = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties
  }

  private val producer = new KafkaProducer[String, String](props)

  def sendTransaction(topic: String, transactionId: String, cardNumber: String, amount: Double, timestamp: Timestamp, location: String): Try[Unit] = {
    val message = s"$transactionId,$cardNumber,$amount,$timestamp,$location"
    val record = new ProducerRecord[String, String](topic, "key", message)
    println(s"Sending transaction to topic $topic: $message")
    Try(producer.send(record))
  }

  def generateAndSendTransaction(topic: String): Unit = {
    val transactionId = UUID.randomUUID().toString
    val cardNumber = f"4000${Random.nextInt(1000000000)}%010d"
    val amount = Random.nextDouble() * 1000
    val timestamp = new Timestamp(System.currentTimeMillis())
    val locations = List("Madrid", "Bilbao", "Sevilla", "Valencia", "Barcelona")
    val location = locations(Random.nextInt(locations.size))

    sendTransaction(topic, transactionId, cardNumber, amount, timestamp, location) match {
      case Success(_) => println(s"Transaction sent successfully to $topic")
      case Failure(ex) => println(s"Failed to send transaction to $topic: ${ex.getMessage}")
    }
  }

  def main(args: Array[String]): Unit = {
    val topic = "debit_card_transactions"
    val delayBetweenMessages = 5000

    while (true) {
      generateAndSendTransaction(topic)
      Thread.sleep(delayBetweenMessages)
    }
    producer.close()
  }
}


