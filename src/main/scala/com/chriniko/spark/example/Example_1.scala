package com.chriniko.spark.example

import java.util.Properties

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

class Example_1 extends Example {

  override def run(): Unit = {
    val sparkSession =SparkSession.builder
      .master("local")
      .appName("carsExample")
      .config("spark.some.config.option", "some-value")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.test")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    val sqlContext: SQLContext = sparkSession.sqlContext

    val customSchema: StructType = StructType(
      Array(
        StructField("make", StringType, nullable = true),
        StructField("mpg", DoubleType, nullable = true),
        StructField("cyl", IntegerType, nullable = true),
        StructField("disp", DoubleType, nullable = true),
        StructField("hp", IntegerType, nullable = true),
        StructField("drat", DoubleType, nullable = true),
        StructField("wt", DoubleType, nullable = true),
        StructField("qsec", DoubleType, nullable = true),
        StructField("vs", IntegerType, nullable = true),
        StructField("am", IntegerType, nullable = true),
        StructField("gear", IntegerType, nullable = true),
        StructField("carb", IntegerType, nullable = true)
      )
    )

    val filename: String = "/cars.csv"
    val pathToFile: String = getClass.getResource(filename).toString

    println(pathToFile)

    val df: DataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // use first line as headers
      .option("inferSchema", "true") // automatically infer data types
      //.schema(customSchema)
      .csv(pathToFile)

    println("Starting...")

    // Resilient Distributed Dataset (RDD)
    df.rdd.cache()

    println("\nPartitions")
    df.rdd.partitions.foreach(println)
    println("\n")

    // Note: print every fetched record and the schema...
    df.rdd.foreach(println)
    df.printSchema()

    // Note: create temp table in order to execute queries...
    df.registerTempTable("cars")

    // 1st example
    println("\n\n")
    sqlContext
      .sql("select * from cars")
      .collect()
      .foreach(println)

    // 2nd example
    println("\n\n")
    sqlContext
      .sql("select cyl, count(cyl) from cars group by cyl")
      .collect()
      .foreach(println)

    // 3rd example
    println("\n\n")
    df.groupBy("cyl")
      .agg(count("*").alias("cnt"))
      .where("`cnt` >= 1")
      .show()

    // Save to MySQL Section
    val url = "jdbc:mysql://localhost:3306/db"

    val properties = new Properties()
    properties.setProperty("user", "user")
    properties.setProperty("password", "password")

    df.write.mode(SaveMode.Overwrite).jdbc(url, "cars", properties)

    df.groupBy("cyl")
      .agg(count("*").alias("cnt"))
      .where("`cnt` >= 1")
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(url, "cars_count_by_cyl", properties)


    // Save to MongoDB
    MongoSpark.save(df)

  }


}
