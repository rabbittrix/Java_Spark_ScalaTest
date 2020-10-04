package com.jrsf.altran

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

object SourcesCSV {
  /**
   * @author Roberto de Souza
   * Delete events between maintenance or ENSAIO: how far back we want to look in order to find an ‘FIM’ event.
   * @param args
   */
  def main(args: Array[String]): Unit = {
    var timeExecution = System.currentTimeMillis
    val conf = new SparkConf().setAppName("SourcesCSV").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val spark = SparkSession.builder.appName("Delete Between Start to End").getOrCreate()
    val customizedSchema = StructType(Array(StructField("Tag[14-16]", StringType, true),
      StructField("Descritivo1", StringType, true),
      StructField("Ensaio_E", StringType, true),
      StructField("Date", StringType, true),
      StructField("Time", StringType, true),
      StructField("Key", StringType, true)))
    val pathToFile = "src\\main\\Resources\\delete_between_inicio_fim.csv"
    val DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customizedSchema).load(pathToFile)
    print("We are starting the test......!!!")
    DF.rdd.cache()
    DF.rdd.foreach(println)
    println(DF.printSchema)
    DF.registerTempTable("ENSAIO")

    sqlContext.sql("select * from ENSAIO").show()
    sqlContext.sql("select * from ENSAIO where Ensaio_E = 'INICIO'").show()
    //sqlContext.sql("SELECT to_json(named_struct('time', to_timestamp('2015-08-26', 'yyyy-MM-dd')), map('timestampFormat', 'dd/MM/yyyy'))").show()
    /*
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
     */
    println("We are finished....!!!")
    printf("Duration of execution: %.3f ms\n",(System.currentTimeMillis() - timeExecution) / 1000d)
    spark.stop()
  }
}