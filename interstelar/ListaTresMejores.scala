package main.scala.com.bddprocessing.interstelar

import main.scala.com.bddprocessing.commons.Utils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD



object ListaTresMejores {

  def main(args: Array[String]): Unit = {

    //Creamos el Logger para ver los errores, lo ponemos en OFF.
    Logger.getLogger("org").setLevel(Level.OFF)


    //val ssc = new StreamingContext("local[*]",
    //"consumo-medio-kafka",Seconds(1))

    //Creamos el SparkContext()
    val conf = new SparkConf().setAppName("historico-batch").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //Creamos el Spark StreamingContext
    val ssc = new StreamingContext(sc, Seconds(1))

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

    //Creamos el DStream
    val stream = KafkaUtils.createDirectStream[String,String](ssc,PreferConsistent,
      Subscribe[String,String](topics,kafkaParams))

    //HISTÓRICO
    //Ingesta de los datos de consumo medios históricos
    val historicoBatch = sc.textFile("in/historico_batch.csv")
    //val historicoBatch = sc.textFile("out/id_nave_nombre_consumo_medio.csv/part-00000.txt")


    //Los datos de historicoBatch los pasamos a un map para obtener tuplas (K,V)
    val requestHistoricoBatch = historicoBatch.map(z =>
      (z.split(",")(1).toString, z.split(",")(2).toDouble))


    //Creamos una variable broadcast con el histórico de consumos medio
    var historicoBatchBc = sc.broadcast(requestHistoricoBatch.collect.toMap)


    //STREAMING

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
    val consumoTotalNaveStream = request.reduceByKey((x,y) =>(x._1 + y._1 , x._2 + y._2))



    //Hacemos un mapValues para obtener de cada Key -->
    // V: valores medios de consumo en streaming
    val consumoMedioNavesStream = consumoTotalNaveStream.mapValues(x => x._2/x._1)



    //Creamos una variable agruparConsumoHistRT
    // para agrupar la (K,(Value,otherValue:valor de la variable broadcast)
    val agruparConsumoHistRT = consumoMedioNavesStream
      .flatMap{case (key,value) =>
        historicoBatchBc.value.get(key).map{otherValue =>
          (key,(otherValue,value))
        }
      }


    //Calculamos las diferencias de los consumos: (históricos - real time)
    val difConsumoHistRT = agruparConsumoHistRT.mapValues(y => y._1 - y._2)



    //Hacemos las siguientes Transformaciones:
    // 1. Intercambio de (K:id_nave,V:diferencia_consumo) a (K:diferencia_consumo,V:id_nave)
    // 2. transform: nos permite aplicar una Transformación subyacente sobre rdd
    //    en este caso le aplicamos SortByKey con ascending = false para ordenar
    //    de mayor a menor
    // 3. Intercambio de (K:diferencia_consumo,V:id_nave,) a (K:id_nave,V:diferencia_consumo)

    val sorted = difConsumoHistRT.map(x => x.swap)
                                       .transform(rdd => rdd.sortByKey(false))
                                        .map(pareja => pareja.swap)


    //Pasamos la diferencia de consumos por un reduceByKeyAndWindow con:
    // Window: 300 sg
    // Slide window: 1 sg
    val tresMejoresNaves = sorted.reduceByKeyAndWindow(_ + _,_ - _,Seconds(300),Seconds(1))



    //Imprimimos los resultados de las 3 naves con los mejores consumos cada sg
    tresMejoresNaves.foreachRDD((rdd,time) =>{
      println(s"Top 3 naves: ${time}")
      rdd.take(3).foreach(
        pair => printf(s"Id_nave: %s , Consumo_dif:(%s) \n",pair._1, pair._2)
      )
    })


    //Grabamos los datos de resultados de las 3 naves con los mejores consumos cada sg
    tresMejoresNaves.saveAsTextFiles("out/tres_mejores_naves_rt.csv")

    //Activamos el proceso Spark Streaming Context
    ssc.start()

    //Llamamos a awaitTerminatio() para esperar
    // la terminación del proceso de computación de Streaming.
    ssc.awaitTermination()


  }

}

