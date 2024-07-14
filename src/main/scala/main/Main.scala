package main

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("DebitCardTransactionConsumer")
      .master("local[*]")
      .getOrCreate()

    // Establecer el nivel de log a FATAL
    spark.sparkContext.setLogLevel("FATAL")

    import spark.implicits._

    val kafkaBootstrapServers = "localhost:9092"
    val topic = "debit_card_transactions"

    val kafkaDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()

    val df = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]

    val transactionDF = df
      .select(split($"value", ",").as("values"))
      .select(
        $"values".getItem(0).as("transactionId"),
        $"values".getItem(1).as("cardNumber"),
        $"values".getItem(2).cast("double").as("amount"),
        $"values".getItem(3).cast("timestamp").as("timestamp"),
        $"values".getItem(4).as("location")
      )

    val jsonQuery = transactionDF.writeStream
      .outputMode("append")
      .format("json")
      .option("path", "/home/kafka/workspace/Data-Engineering-Portfolio-Streaming/out/json")
      .option("checkpointLocation", "/home/kafka/workspace/Data-Engineering-Portfolio-Streaming/out/checkpoint")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // Calcular la media del importe para cada ciudad
    val avgAmountDF = transactionDF
      .groupBy($"location")
      .agg(avg($"amount").as("average_amount"))

    val avgQuery = avgAmountDF.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    jsonQuery.awaitTermination()
    avgQuery.awaitTermination()
  }
}




