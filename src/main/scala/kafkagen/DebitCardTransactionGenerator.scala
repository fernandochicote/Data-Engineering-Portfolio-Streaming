package kafkagen

import java.util.{Properties, UUID}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.sql.Timestamp
import scala.util.{Try, Success, Failure}
import scala.util.Random
import config.config

object DebitCardTransactionGenerator {

  // Configuración de las propiedades del productor de Kafka
  private val props: Properties = {
    val properties = new Properties()
    properties.put("bootstrap.servers", config.kafkaBootstrapServers)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties
  }

  // Creación del productor de Kafka
  private val producer = new KafkaProducer[String, String](props)

  // Función para enviar una transacción a Kafka
  private def sendTransaction(topic: String, transactionId: String, cardNumber: String, amount: Double, timestamp: Timestamp, location: String, currency: String): Try[Unit] = {
    val message = s"$transactionId,$cardNumber,$amount,$timestamp,$location,$currency"
    val record = new ProducerRecord[String, String](topic, "key", message)
    println(s"Sending transaction to topic $topic: $message")
    Try(producer.send(record))
  }

  // Función para generar y enviar una transacción aleatoria
  private def generateAndSendTransaction(topic: String): Unit = {
    val transactionId = UUID.randomUUID().toString
    val cardNumber = f"4000${Random.nextInt(1000000000)}%010d"
    val amount = BigDecimal(0.01 + (Random.nextDouble() * 999.99)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    val timestamp = new Timestamp(System.currentTimeMillis())
    val locations = List("Madrid", "Bilbao", "Sevilla", "Valencia", "Barcelona")
    val location = locations(Random.nextInt(locations.size))
    val currencies = List("USD", "EUR", "GBP")
    val currency = currencies(Random.nextInt(currencies.size))

    // Enviar la transacción a Kafka y manejar el resultado
    sendTransaction(topic, transactionId, cardNumber, amount, timestamp, location, currency) match {
      case Success(_) => println(s"Transaction sent successfully to $topic")
      case Failure(ex) => println(s"Failed to send transaction to $topic: ${ex.getMessage}")
    }
  }

  // Método principal para ejecutar el generador de transacciones
  def main(args: Array[String]): Unit = {
    val topic = config.topic
    val delayBetweenMessages = 5000
    val maxTransactions = 10000
    var transactionCount = 0

    // Bucle para generar y enviar transacciones hasta alcanzar el número máximo
    while (transactionCount < maxTransactions) {
      generateAndSendTransaction(topic)
      transactionCount += 1
      Thread.sleep(delayBetweenMessages)
    }
    producer.close()
  }
}





