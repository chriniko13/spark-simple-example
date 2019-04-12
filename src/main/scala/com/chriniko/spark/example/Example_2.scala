package com.chriniko.spark.example

import java.io._
import java.nio.file.{Files, Paths}
import java.util.Properties
import java.util.concurrent.CountDownLatch
import java.util.zip.GZIPInputStream

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import org.apache.spark.sql.functions._

class Example_2 extends Example {

  override def run(): Unit = {

    val recreateDecompressedFilesIfAlreadyExist = false

    val decompressionDetails: List[(String, String)] = decompressTsvFiles(recreateDecompressedFilesIfAlreadyExist)

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("imdbCsvsExample")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    val sqlContext: SQLContext = sparkSession.sqlContext

    val decompressedImdbFiles: List[String] = decompressionDetails.map(_._2).sorted

    val writeToDatabase = false

    val url = "jdbc:mysql://localhost:3306/db"

    val properties = new Properties()
    properties.setProperty("user", "user")
    properties.setProperty("password", "password")


    println("\n~~~ Name Basics Tsv Loading ~~~")
    val nameBasicsDf = loadNameBasicsTsv(sqlContext, decompressedImdbFiles(0))
    //nameBasicsDf.persist(StorageLevel.MEMORY_AND_DISK)
    nameBasicsDf.cache()

    println("\n~~~ Title Akas Tsv Loading ~~~")
    val titleAkasDf = loadTitleAkasTsv(sqlContext, decompressedImdbFiles(1))
    //titleAkasDf.persist(StorageLevel.MEMORY_AND_DISK)
    titleAkasDf.cache()

    println("\n~~~ Title Basics Tsv Loading ~~~")
    val titleBasicsDf = loadTitleBasicsTsv(sqlContext, decompressedImdbFiles(2))
    //titleBasicsDf.persist(StorageLevel.MEMORY_AND_DISK)
    titleBasicsDf.cache()

    println("\n~~~ Title Crew Tsv Loading ~~~")
    val titleCrewDf = loadTitleCrewTsv(sqlContext, decompressedImdbFiles(3))
    //titleCrewDf.persist(StorageLevel.MEMORY_AND_DISK)
    titleCrewDf.cache()

    println("\n~~~ Title Episode Tsv Loading ~~~")
    val titleEpisodeDf = loadTitleEpisodeTsv(sqlContext, decompressedImdbFiles(4))
    //titleEpisodeDf.persist(StorageLevel.MEMORY_AND_DISK)
    titleEpisodeDf.cache()

    println("\n~~~ Title Principals Tsv Loading ~~~")
    val titlePrincipalsDf = loadTitlePrincipalsTsv(sqlContext, decompressedImdbFiles(5))
    //titlePrincipalsDf.persist(StorageLevel.MEMORY_AND_DISK)
    titlePrincipalsDf.cache()

    println("\n~~~ Title Ratings Tsv Loading ~~~")
    val titleRatingsDf = loadTitleRatingsTsv(sqlContext, decompressedImdbFiles(6))
    //titleRatingsDf.persist(StorageLevel.MEMORY_AND_DISK)
    titleRatingsDf.cache()

    val runSomeSampleQueries = false
    if (runSomeSampleQueries) {
      // Join Example 1
      titleBasicsDf.join(
        titleAkasDf,
        col("tconst") === col("titleId"),
        "left")
        .limit(5)
        .show()

      println("\n")

      // Join Example 2
      sqlContext.sql(
        """select *
         from title_basics join title_akas
         where title_basics.tconst == title_akas.titleId""")
        .limit(5)
        .show()
    }

    // Complete Info show example...
    //val titleToSearchFor = "%Ocean's Eleven%"
    val titleToSearchFor = "%Spiderman%"


    val titleBasicsSearchResults = titleBasicsDf
      .filter(
        col("primaryTitle").like(titleToSearchFor)
          .or(col("originalTitle").like(titleToSearchFor))
      )
      .limit(3)
      .collect()


    for (titleBasicsSearchResult <- titleBasicsSearchResults) {

      println("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

      val tconst = titleBasicsSearchResult.getString(0)
      println("\n--- Title Basic Info ---")
      println(titleBasicsSearchResult)

      // fetch title akas
      println("\n--- Title Akas Info ---")
      val titleAkas = titleAkasDf.filter(col("titleId") === tconst).collect()
      titleAkas.foreach(println)

      println("\n--- Title Ratings ---")
      val titleRatings = titleRatingsDf.filter(col("tconst") === tconst).collect()
      titleRatings.foreach(println)

      println("\n--- Title Episode ---")
      val titleEpisodes = titleEpisodeDf.filter(col("tconst") === tconst).collect()

      titleEpisodes.foreach(t => {
        println(t)

        val parentTconst = t.getString(1)

        val parentTitleEpisodes = titleEpisodeDf.filter(col("parentTconst") === parentTconst)
          .orderBy(asc("seasonNumber"), asc("episodeNumber"))
          .collect()

        //TODO fix error...
        println("\nParent(s):")
        parentTitleEpisodes.foreach(println)
      })

      println("\n--- Title Crew ---")
      val titleCrew = titleCrewDf.filter(col("tconst") === tconst).collect()
      titleCrew.foreach(tC => {

        println("\nDirectors:")
        val directorsNconsts = tC.getString(1)
        if (directorsNconsts != "\\N") {

          directorsNconsts.split(",").foreach(nconst => {

            val directors = nameBasicsDf.filter(col("nconst") === nconst).collect()
            directors.foreach(println)
          })
        }

        println("\nWriters:")
        val writersNconsts = tC.getString(2)
        if (writersNconsts != "\\N") {

          writersNconsts.split(",").foreach(nconst => {

            val writers = nameBasicsDf.filter(col("nconst") === nconst).collect()
            writers.foreach(println)
          })
        }

      })

      println("\n--- Title Principals ---")
      val titlePrincipals = titlePrincipalsDf.filter(col("tconst") === tconst).collect()
      titlePrincipals.foreach(tPrincipal => {

        println(tPrincipal)

        val nconst = tPrincipal.getString(2)
        val principalNames = nameBasicsDf.filter(col("nconst") === nconst).collect()

        if (principalNames.length != 1) {
          throw new IllegalStateException()
        }

        println(principalNames.head)


        println
      })

    }


    if (writeToDatabase) {
      nameBasicsDf.write.option("numPartitions", 10).mode(SaveMode.Overwrite).jdbc(url, "name_basics", properties)

      titleAkasDf.write.option("numPartitions", 10).mode(SaveMode.Overwrite).jdbc(url, "title_akas", properties)

      titleBasicsDf.write.option("numPartitions", 10).mode(SaveMode.Overwrite).jdbc(url, "title_basics", properties)

      titleCrewDf.write.option("numPartitions", 10).mode(SaveMode.Overwrite).jdbc(url, "title_crew", properties)

      titleEpisodeDf.write.option("numPartitions", 10).mode(SaveMode.Overwrite).jdbc(url, "title_episode", properties)

      titlePrincipalsDf.write.option("numPartitions", 10).mode(SaveMode.Overwrite).jdbc(url, "title_principals", properties)

      titleRatingsDf.write.option("numPartitions", 10).mode(SaveMode.Overwrite).jdbc(url, "title_ratings", properties)
    }

    println("Job Finished!")
  }

  def loadNameBasicsTsv(sqlContext: SQLContext, file: String): DataFrame = {
    val nameBasicsDf: DataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // use first line as headers
      .option("inferSchema", "true") // automatically infer data types
      .option("delimiter", "\t")
      .csv(file)

    println(s"nameBasicsDf size: ${nameBasicsDf.count()}")
    nameBasicsDf.printSchema()
    nameBasicsDf.createOrReplaceTempView("name_basics")

    nameBasicsDf
  }


  def loadTitleAkasTsv(sqlContext: SQLContext, file: String): DataFrame = {

    val titleAkasDf: DataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // use first line as headers
      .option("inferSchema", "true") // automatically infer data types
      .option("delimiter", "\t")
      .csv(file)

    println(s"titleAkasDf size: ${titleAkasDf.count()}")
    titleAkasDf.printSchema()
    titleAkasDf.createOrReplaceTempView("title_akas")

    titleAkasDf
  }

  def loadTitleBasicsTsv(sqlContext: SQLContext, file: String): DataFrame = {
    val titleBasicsDf: DataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // use first line as headers
      .option("inferSchema", "true") // automatically infer data types
      .option("delimiter", "\t")
      .csv(file)

    println(s"titleBasicsDf size: ${titleBasicsDf.count()}")
    titleBasicsDf.printSchema()
    titleBasicsDf.createOrReplaceTempView("title_basics")

    titleBasicsDf
  }

  def loadTitleCrewTsv(sqlContext: SQLContext, file: String): DataFrame = {
    val titleCrewDf: DataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // use first line as headers
      .option("inferSchema", "true") // automatically infer data types
      .option("delimiter", "\t")
      .csv(file)

    println(s"titleCrewDf size: ${titleCrewDf.count()}")
    titleCrewDf.printSchema()
    titleCrewDf.createOrReplaceTempView("title_crew")

    titleCrewDf
  }

  def loadTitleEpisodeTsv(sqlContext: SQLContext, file: String): DataFrame = {
    val titleEpisodeDf: DataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // use first line as headers
      .option("inferSchema", "true") // automatically infer data types
      .option("delimiter", "\t")
      .csv(file)

    println(s"titleEpisodeDf size: ${titleEpisodeDf.count()}")
    titleEpisodeDf.printSchema()
    titleEpisodeDf.createOrReplaceTempView("title_episode")

    titleEpisodeDf
  }

  def loadTitlePrincipalsTsv(sqlContext: SQLContext, file: String): DataFrame = {
    val titlePrincipalsDf: DataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // use first line as headers
      .option("inferSchema", "true") // automatically infer data types
      .option("delimiter", "\t")
      .csv(file)

    println(s"titlePrincipalsDf size: ${titlePrincipalsDf.count()}")
    titlePrincipalsDf.printSchema()
    titlePrincipalsDf.createOrReplaceTempView("title_principals")

    titlePrincipalsDf
  }

  def loadTitleRatingsTsv(sqlContext: SQLContext, file: String): DataFrame = {
    val titleRatingsDf: DataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // use first line as headers
      .option("inferSchema", "true") // automatically infer data types
      .option("delimiter", "\t")
      .csv(file)

    println(s"titleRatingsDf size: ${titleRatingsDf.count()}")
    titleRatingsDf.printSchema()
    titleRatingsDf.createOrReplaceTempView("title_ratings")

    titleRatingsDf
  }

  private def decompressTsvFiles(recreateDecompressedFilesIfAlreadyExist: Boolean): List[(String, String)] = {

    val prePath = "/home/giatros/work_projects/spark-simple-example/"

    val countDownLatch = new CountDownLatch(7 /*total number of files*/)

    val decompressionDetails = List(
      Tuple2(prePath + "src/main/resources/name.basics.tsv.gz", prePath + "name.basics.tsv"),
      Tuple2(prePath + "src/main/resources/title.akas.tsv.gz", prePath + "title.akas.tsv"),
      Tuple2(prePath + "src/main/resources/title.basics.tsv.gz", prePath + "title.basics.tsv"),
      Tuple2(prePath + "src/main/resources/title.crew.tsv.gz", prePath + "title.crew.tsv"),
      Tuple2(prePath + "src/main/resources/title.episode.tsv.gz", prePath + "title.episode.tsv"),
      Tuple2(prePath + "src/main/resources/title.principals.tsv.gz", prePath + "title.principals.tsv"),
      Tuple2(prePath + "src/main/resources/title.ratings.tsv.gz", prePath + "title.ratings.tsv")
    )

    if (recreateDecompressedFilesIfAlreadyExist) {

      decompressionDetails.foreach(t => {

        Future {
          println(s"  >>> will uncompress ${t._1} to ${t._2}")

          val iterator = GzFileIterator(new java.io.File(t._1), "UTF-8")
          Extractor(iterator, t._2)

          println(s"  >>> uncompressed ${t._1} to ${t._2}")

          countDownLatch.countDown()
        }

      })

      countDownLatch.await()
      println("all files decompressed...")
    }

    decompressionDetails
  }

  // ----------------------- UTILS -------------------------------------------------------------------------------------
  object Extractor {

    def apply(iterator: Iterator[String], outputPath: String): Unit = {

      val file = new File(outputPath)
      val path = Paths.get(outputPath)

      Files.deleteIfExists(path)
      Files.createFile(path)

      val bw = new BufferedWriter(new FileWriter(file))

      iterator.foreach(line => {
        bw.write(line)
        bw.newLine()
      })

      bw.close()
    }

  }

  class BufferedReaderIterator(reader: BufferedReader) extends Iterator[String] {

    override def hasNext(): Boolean = reader.ready

    override def next(): String = reader.readLine()
  }

  object GzFileIterator {
    def apply(file: java.io.File, encoding: String): BufferedReaderIterator = {
      new BufferedReaderIterator(
        new BufferedReader(
          new InputStreamReader(
            new GZIPInputStream(
              new FileInputStream(file)), encoding)))
    }
  }

}
