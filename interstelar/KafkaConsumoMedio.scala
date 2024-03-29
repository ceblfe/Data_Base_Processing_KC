package main.scala.com.bddprocessing.interstelar

import main.scala.com.bddprocessing.commons.Utils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe






object KafkaConsumoMedio {

  def main(args: Array[String]): Unit = {

    //Creamos el Logger para ver los errores, lo ponemos en OFF.
    Logger.getLogger("org").setLevel(Level.OFF)


    //Creamos el Spark StreamingContext
    val ssc = new StreamingContext("local[*]",
      "consumo-medio-kafka",Seconds(1))

    //Creamos un checkpoint para asegurar la fiabilidad del sistema
    ssc.checkpoint("out/checkpoint")


    //Definimos los parámetros de kafka
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //Definimos el topic al que me quiero subscribir como consumer (driver program)
    //a que topics me quiero subscribir como consumer (driver program)
    val topics = Array("interestelar")

    //crear el DStream
    val stream = KafkaUtils.createDirectStream[String,String](ssc,PreferConsistent,
      Subscribe[String,String](topics,kafkaParams))


    //Creamos la variable lines para obtener en un map
    // cada línea del proceso de Streaming
    val lines = stream.map(item => item.value)

    //Dividimos cada línea en una tupla con:
    // (K: id nave, V:(contador, consumo medio en streaming))
    val request = lines.map(linea =>
      (linea.split(",")(1).toString,
        (1,linea.split(",")(9).toDouble)))


    //Hacemos un reduceByKey para obtener de cada Key los valores:
    // x._1 + y._1 --> suma de los contadores
    // x._2 + y._2 --> suma de los consumos medios en streaming
    val consumoTotalNaveStream = request.reduceByKey((x,y) =>(x._1 + y._1,x._2 + y._2))


    //Hacemos un mapValues para obtener de cada Key -->
    // V: valores medios de consumo en streaming
    val consumoMedioNavesStream = consumoTotalNaveStream.mapValues(x => x._2/x._1)


    //Pasamos los consumos medios en streaming por un reduceByKeyAndWindow con:
    // Window: 300 sg
    // Slide window: 1 sg
    val resultsStream = consumoMedioNavesStream.reduceByKeyAndWindow(_ + _,_ - _,Seconds(300),Seconds(1))

    //Imprimimos los resultsStream
    resultsStream.print()

    resultsStream.saveAsTextFiles("out/consumos_medios_rt.csv")

    //Activamos el proceso Spark Streaming Context
    ssc.start()

    //Llamamos a awaitTerminatio() para esperar
    // la terminación del proceso de computación de Streaming.
    ssc.awaitTermination()

  }

}