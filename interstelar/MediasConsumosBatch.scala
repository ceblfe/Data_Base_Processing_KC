package main.scala.com.bddprocessing.interstelar

import breeze.linalg.shuffle
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD


object MediasConsumosBatch {

  //creacion de las case class naveConsumos y naveNombre
  case class naveConsumos(id_nave:String,consumo_medio:Double)
  case class naveNombre(id_nave:String,nombre:String)

  def main(args: Array[String]): Unit = {


    //Cargamos el Logger para poder los errores que se van generando
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Creamos la variable SparkSession
    val spark = SparkSession.builder()
      .appName("users-data")
      .config("spark.master","local[*]")
      .getOrCreate()

    import spark.implicits._

    //Hacemos la ingesta de los datos correspondientes:
    // * Tabla que contiene los datos de id_nave y consumo de coltanita-->val navesConsumo
    // * Tabla que contiene los datos de id_nave y nombre de nave-->val idNaveNombre
    val navesConsumo = spark.read
      .option("header","false")
      .option("inferSchema",value = true)
      .csv("in/trayectos.csv")

    val idNaveNombre = spark.read
      .option("header","true")
      .option("inferSchema",value = true)
      .csv("in/naves_transporte.csv")



    //Mostramos las variables de la ingesta tabulados
    navesConsumo.show()
    idNaveNombre.show()

    //Inferimos un esquema de cada tabla para mostras de que tipo son los datos
    navesConsumo.printSchema()
    idNaveNombre.printSchema()

    //Limpiamos la cabecera de la variable idNaveNombre.
    //lo tabulamos en formato de DataFrame
    val cleanIdNaveNombre = idNaveNombre.dropDuplicates(List("Codigo"))
      .toDF("id_nave".toString,"nombre_nave".toString)


    //Seleccionamos las columnas "_c1" y "_c9" de la tabla navesConsumo
    val df1 = navesConsumo.select($"_c1",$"_c9")


    //En navesConsumo hacemos las siguiente:
    // * Transformación mediante un groupby de la columna _c1
    // * Usamos la función agg de la API de sql de Spark SQL para hacer
    //   la media de cada _c1.
    // * Pasamos a dataFrame la tabla renombrando las columnas
    val df2 = navesConsumo.groupBy("_c1").agg(avg(df1("_c9")))
      .toDF("id_nave".toString,"consumo_medio".toString)


    //Creamos la variable agrupacion en el que hacemos un join entre
    //  cleanIdNaveNombre y df2.
    //Eliminamos a continuación la columna "id_nave" de cleanIdNaveNombre
    //  que esta en la tabla agrupacion.
    val agrupacion = cleanIdNaveNombre.join(df2,cleanIdNaveNombre("id_nave") === df2("id_nave"),
      "inner").drop(cleanIdNaveNombre("id_nave"))


    //Transformamos el DataFrame agrupacion a RDD
    val agrupacionRdd = agrupacion.rdd

    //Grabamos los datos en la carpeta out/ en formato .csv
    //agrupacion.write.csv("out/id_nave_nombre_consumo_medio.csv")


    //Grabamos en una partición los resultados
    agrupacionRdd.coalesce(1,shuffle = true).saveAsTextFile("out/id_nave_nombre_consumo_medio.csv")



    //Paramos sparkSession
    spark.stop()



  }




}
