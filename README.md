# Portafolio de Ingeniería de Datos - Procesamiento de Datos en Streaming

## Semana 1: Introducción a Apache Kafka y su ecosistema 

Durante la primera semana, exploramos Apache Kafka y su ecosistema. Aprendimos sobre los conceptos básicos de Kafka, cómo instalarlo y configurarlo, y cómo utilizar sus componentes principales: Producers, Consumers, Topics y Brokers. También discutimos casos de uso comunes y cómo Kafka se integra en un sistema de procesamiento de datos en tiempo real.


## Semana 2: Introducción a Spark Structured Streaming 

En la segunda semana, nos adentramos en Apache Spark y su módulo de Structured Streaming. Spark Structured Streaming es una API para el procesamiento de flujos de datos en tiempo real que se construye sobre el motor de Spark SQL. Aprendimos a procesar datos en tiempo real utilizando Datasets y DataFrames, así como a integrar Spark con Kafka para consumir y procesar datos en streaming.

## Generador de Datos: `DebitCardTransactionGenerator.scala`

El `DebitCardTransactionGenerator` es una aplicación Scala que simula la generación de transacciones de tarjeta de débito y las envía a un tópico de Kafka.

### Descripción General:

- **Configuración de Kafka**: Configura las propiedades del productor de Kafka, incluyendo el servidor bootstrap y los serializadores de clave y valor.
- **Generación de Transacciones**: Genera transacciones aleatorias con datos como ID de transacción, número de tarjeta, importe, marca temporal, ubicación y moneda.
- **Envío a Kafka**: Envía las transacciones generadas al tópico especificado en Kafka.
- **Ciclo Continuo**: Realiza este proceso en un ciclo continuo, respetando un intervalo de tiempo configurado entre cada transacción.

## Consumidor de Datos: `Main.scala`

El `Main` es una aplicación Scala que consume datos de transacciones de tarjeta de débito desde Kafka utilizando Spark Structured Streaming. Procesa los datos en tiempo real, calcula la media de los importes por ciudad y guarda los datos en formato JSON.

### Descripción General:

- **Configuración de Spark y Kafka**: Configura las propiedades de Spark y Kafka para la lectura del stream.
- **Procesamiento de Transacciones**: Lee las transacciones desde Kafka, las convierte en un DataFrame y las descompone en columnas individuales.
- **Cálculo de Media por Ciudad**: Agrupa las transacciones por ciudad y calcula la media de los importes.
- **Almacenamiento en JSON**: Guarda todas las transacciones en formato JSON en una ubicación especificada.
- **Streaming Continuo**: Ejecuta estas operaciones en un stream continuo con un intervalo de tiempo configurado.


