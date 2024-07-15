package main

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import config.config
import org.apache.spark.sql.streaming.GroupState
import org.apache.spark.sql.streaming.GroupStateTimeout

object Main {
  // Define una case class para representar el estado de cada ciudad
  private case class CityState(location: String, maxAmount: Double, minAmount: Double)

  def main(args: Array[String]): Unit = {
    // Crea una sesión de Spark
    val spark = SparkSession.builder
      .appName("DebitCardTransactionConsumer")
      .master("local[*]")
      .getOrCreate()

    // Establece el nivel de log a FATAL para reducir la verbosidad de los logs
    spark.sparkContext.setLogLevel("FATAL")

    import spark.implicits._

    // Lee el stream de Kafka
    val kafkaDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBootstrapServers)
      .option("subscribe", config.topic)
      .option("startingOffsets", "latest")
      .option("kafkaConsumer.pollTimeoutMs", "10000")
      .load()

    // Convierte el valor del DataFrame de Kafka a String
    val df = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]

    // Divide los valores por comas y los asigna a las respectivas columnas
    val transactionDF = df
      .select(split($"value", ",").as("values"))
      .select(
        $"values".getItem(0).as("transactionId"),
        $"values".getItem(1).as("cardNumber"),
        $"values".getItem(2).cast("double").as("amount"),
        $"values".getItem(3).cast("timestamp").as("timestamp"),
        $"values".getItem(4).as("location"),
        $"values".getItem(5).as("currency")
      )

    // Lee los tipos de cambio desde un archivo JSON
    val exchangeRatesPath = config.exchangesRates
    val exchangeRatesDF = spark.read.json(exchangeRatesPath)

    // Enriquece el DataFrame de transacciones con los tipos de cambio
    val enrichedDF = transactionDF.join(broadcast(exchangeRatesDF), "currency")
      .withColumn("amount_in_eur", round($"amount" * $"rate_to_eur", 2))

    // Muestra los datos enriquecidos por consola
    val consoleQuery = enrichedDF.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", value = false)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // Guarda los datos enriquecidos en formato JSON
    val jsonQuery = enrichedDF.writeStream
      .outputMode("append")
      .format("json")
      .option("path", config.outputJson)
      .option("checkpointLocation", config.checkpoint)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // Calcula la media del importe en euros para cada ciudad y redondea a dos decimales
    val avgAmountDF = enrichedDF
      .groupBy($"location")
      .agg(round(avg($"amount_in_eur"), 2).as("average_amount_in_eur"))

    // Muestra la media del importe por ciudad en la consola
    val avgQuery = avgAmountDF.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", value = false)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // Define la función de actualización del estado
    def updateStateWithEvent(location: String, inputs: Iterator[Row], oldState: GroupState[CityState]): CityState = {
      // Obtiene el estado actual o crea uno nuevo si no existe
      val state = if (oldState.exists) oldState.get else CityState(location, Double.MinValue, Double.MaxValue)
      // Actualiza el estado con los nuevos datos
      val updatedState = inputs.foldLeft(state) { (currentState, row) =>
        val amountInEur = row.getAs[Double]("amount_in_eur")
        CityState(
          location,
          Math.max(currentState.maxAmount, amountInEur),
          Math.min(currentState.minAmount, amountInEur)
        )
      }
      // Guarda el nuevo estado
      oldState.update(updatedState)
      // Imprime el nuevo estado para depuración
      println(s"Updated state for $location: $updatedState")
      updatedState
    }

    // Agrupa los datos por ciudad y aplica la función de actualización del estado
    val stateDF = enrichedDF
      .groupByKey(row => row.getAs[String]("location"))
      .mapGroupsWithState[CityState, CityState](GroupStateTimeout.NoTimeout)(updateStateWithEvent)

    // Muestra los resultados del estado en la consola
    val stateQuery = stateDF.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", value = false)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // Espera a que todas las consultas terminen
    consoleQuery.awaitTermination()
    jsonQuery.awaitTermination()
    avgQuery.awaitTermination()
    stateQuery.awaitTermination()
  }
}
