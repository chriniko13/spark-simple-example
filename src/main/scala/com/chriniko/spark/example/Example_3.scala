package com.chriniko.spark.example

import java.sql.Date
import java.util.UUID

import org.apache.spark.sql.{SQLContext, SparkSession}
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random


class Example_3 extends Example {

  override def run(): Unit = {

    // Note: First populate a table with sample records...
    val db = Database.forConfig("slick.mysql.local")
    try {

      val tickets = (1 to 1000)
        .map(idx => {
          val firstBoard = (1 to 5).map(idx => Random.nextInt(50) + 1).toList.mkString(",")
          val secondBoard = (1 to 5).map(idx => Random.nextInt(50) + 1).toList.mkString(",")

          (UUID.randomUUID().toString, firstBoard, secondBoard, new Date(System.currentTimeMillis()))
        })
        .toList

      val lottoTickets = TableQuery[LottoTickets]

      val setup = DBIO.seq(
        lottoTickets.schema.create,
        lottoTickets ++= tickets
      )

      val setupFuture = db.run(setup.transactionally)
      Await.result(setupFuture, 1.minute)


      val showAllLottoTickets = db
        .run(lottoTickets.result)
        .map(r => r.slice(0, 10).foreach(println))
      Await.result(showAllLottoTickets, 1.minute)

      println
      /*
      val showAllLottoTicketsStreaming = db
          .stream(lottoTickets.result)
          .foreach(println)
      Await.result(showAllLottoTicketsStreaming, 1.minute)
      */


      // Note: start spark job...
      val sparkSession = SparkSession.builder
        .master("local")
        .appName("lottoTicketsExample")
        .getOrCreate()

      sparkSession.sparkContext.setLogLevel("ERROR")

      val sqlContext: SQLContext = sparkSession.sqlContext


      val prop = new java.util.Properties()
      prop.put("user", "user")
      prop.put("password", "password")
      val url = "jdbc:mysql://localhost:3306/db"

      val df = sqlContext.read.jdbc(url, "LOTTO_TICKETS", prop)
      df.show()


      Await.result(db.run(lottoTickets.schema.drop), 1.minute)

    } finally db.close
  }


  // ---------------------------- TABLES --------------------------------------------------------

  class LottoTickets(tag: Tag) extends Table[(String, String, String, Date)](tag, "LOTTO_TICKETS") {
    def ticketId = column[String]("ID", O.PrimaryKey)

    def firstBoard = column[String]("FIRST_BOARD")

    def secondBoard = column[String]("SECOND_BOARD")

    def creationDate = column[Date]("CREATION_DATE")

    override def * = (ticketId, firstBoard, secondBoard, creationDate)

  }

}
