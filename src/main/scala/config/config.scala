package config

object config {

  // Configuración de Kafka
  val kafkaBootstrapServers = "localhost:9092"
  val topic = "debit_card_transactions"
  val outputJson = "/home/kafka/workspace/Data-Engineering-Portfolio-Streaming/out/json"
  val checkpoint = "/home/kafka/workspace/Data-Engineering-Portfolio-Streaming/out/checkpoint"
  val exchangesRates = "/home/kafka/workspace/Data-Engineering-Portfolio-Streaming/in/exchangeRates.json"
}