package MeasureAggregators

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Period}
object AggregateMeasures {

  def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[DateTime]      =Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))

  def main(args: Array[String]) {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    def conf = new SparkConf().setAppName(AggregateMeasures.getClass.getName)

    val sc = new SparkContext(conf)
    println("Hello, world!")
    sc.setLogLevel("ERROR")
    var startingYear = ""
    var startingMonth = ""
    var startingDay = ""
    var endingYear = ""
    var endingMonth = ""
    var endingDay = ""

    var flagLoop = true
    while (flagLoop) {
      println("give starting date: \"year-month-day\"")
      val buffer = Console.readLine
      if (buffer.split("-").length == 3) {
        startingYear = buffer.split("-")(0)
        startingMonth = buffer.split("-")(1)
        startingDay = buffer.split("-")(2)
        var flag = false

        if (!startingYear.forall(_.isDigit)) {
          println("wrong year")
          flag = true
        }
        if (!startingMonth.forall(_.isDigit)) {
          println("wrong month")
          flag = true
        }
        if (!startingDay.forall(_.isDigit)) {
          println("wrong day")
          flag = true
        }
        if (!flag) flagLoop = false
      }
    }

    val startDate: Date = sdf.parse(startingYear + "-" + startingMonth + "-" + startingDay)
    println(startDate.toString)
    println(sdf.format(startDate))



    flagLoop = true
    while (flagLoop) {
      println("give ending date: \"year-month-day\"")
      val buffer = Console.readLine
      if (buffer.split("-").length == 3) {
        endingYear = buffer.split("-")(0)
        endingMonth = buffer.split("-")(1)
        endingDay = buffer.split("-")(2)
        var flag = false

        if (!endingYear.forall(_.isDigit)) {
          println("wrong year")
          flag = true
        }
        if (!endingMonth.forall(_.isDigit)) {
          println("wrong month")
          flag = true
        }
        if (!endingDay.forall(_.isDigit)) {
          println("wrong day")
          flag = true
        }
        if (!flag) flagLoop = false
      }
    }
    val endDate: Date = sdf.parse(endingYear + "-" + endingMonth + "-" + endingDay)

    val myList = dateRange(new DateTime(startDate), new DateTime(endDate), new Period().withDays(1)).toList.map(_.toDate).map(x => "indexes/" + sdf.format(x))

    val listConcat = myList.mkString(",")

    println("Give entity for search:")
    val entity = Console.readLine

    val indexedRdd = sc.textFile(listConcat )

    println("indexedRdd " + indexedRdd.count())

    println(indexedRdd.filter{x =>
      val entities = x.split("\t")(8)
      entities.contains("Pat_Roberts")
    }.count())

  }

}
