# Data Base Processing

This repository contains the following files to solve 2 problems of Data Base Processing: in batch and in streaming with Spark and we are using Scala like programming language.

The files in this repository are:
  1. in/
  2. interstelar/
  3. interstelar.pdf 
  4. README.md
 
## 1. in/

The file in/ contains all data base that we are using .

The data files are:
  1. historico_batch.csv
  2. naves_transporte.csv
  3. trayectos

  With these data base we are going to simulate the processes: batch and streaming.
 

## 2. interstelar/

This file contains the following files with scala extension:

 * KafkaConsumoMedio.scala
 * KafkaDifConsumo.scala
 * ListaTresMejores.scala
 * MediasConsumosBatch.scala

Every Scala file explain a part of every problem that we want to solve in the file interstelar.pdf

## 3. interstelar.pdf

This file contains all the ask that we are going to answer some questions:

Batch processing (to solve these questions we are using Spark SQL):

* Intake of data stored for years by navigation systems spacecrafts and docking ports (mode batch): we answer this question with the file MediasConsumosBatch.scala
* Data cleanig: we answer this question with the file MediasConsumosBatch.scala
* Calculation of means of comsumption of all the spacecrafts of the fleet grouped by spacecraft (every spacecraft has an identifier):we answer this question with the file MediasConsumosBatch.scala 

Streaming process (to solve these questions we are using Spark Streamining and a Kafka machine): 

* Real time comsumption data (Spark Streaming): we answer this question with the file KafkaConsumoMedio.scala 
* Calculation of means of consumption of all the spacecrafts of the fleet grouped by spacecraft (every spacecraft has an identifier) obtained in real time: we answer this question with the file KafkaConsumoMedio.scala
* Process on both datasets obtaining the difference between average consumption: we answer this question with the file KafkaDifConsumo.scala
* Obtaining a collection (List) of tuple elements (identification spacecraft and model) with the three best transports: we answer this question with the file ListaTresMejores.scala