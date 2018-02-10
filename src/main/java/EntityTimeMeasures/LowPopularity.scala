package EntityTimeMeasures

import java.text.SimpleDateFormat
import java.util.Date

import MeasureAggregators.SingleEntityMeasures
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Period}

import scala.collection.mutable

object LowPopularity {


  def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[DateTime]      =Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))

  def aggregateNextDate(start: DateTime, end: DateTime, granularity: Int) : scala.collection.mutable.MutableList[DateTime] = {
    val dateList = scala.collection.mutable.MutableList[DateTime]()
    var current = start

    while(current.isBefore(end)){
      dateList += current
      current = current.plusDays(granularity)
    }
    dateList += end
  }

  def main(args: Array[String]) {

    val sdfDay: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val sdf3: SimpleDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy")

    def conf = new SparkConf().setAppName(SingleEntityMeasures.getClass.getName)

    val sc = new SparkContext(conf)
    val argsLen = args.length
    val all_values = new mutable.HashMap[String,Double]()
    sc.setLogLevel("ERROR")

    val entity = args(0).toString
    val topK = args(1).toInt
    val startDateTemp = args(2)
    val endDateTemp = args(3)
    val granularity = args(4).toInt
    val inputDirectory = args(5).toString

    val startDate: Date = sdfDay.parse(startDateTemp)
    val endDate:   Date = sdfDay.parse(endDateTemp)


    val myListPerDayGran = aggregateNextDate(new DateTime(startDate).minusDays(1), new DateTime(endDate), granularity)

    for (index <- 0 until myListPerDayGran.length - 1)
    {
      val startTime =  myListPerDayGran(index).plusDays(1).getMillis
      val endTime =  myListPerDayGran(index + 1).getMillis

      val indexedRdd = sc.textFile(inputDirectory).filter{x=>
        val temp: Date = sdf3.parse(x.split("\t")(2))
        temp.getTime >= startTime && temp.getTime <= endTime
      }.cache

      val entitySet = indexedRdd.filter(_.split("\t")(3).contains(entity)).cache

      val totalTweets = indexedRdd.count().toDouble
      val totalUserCnt = indexedRdd.map(_.split("\t")(1)).distinct().count().toDouble

      val entityCnt = entitySet.count().toDouble
      val entityUserCnt = entitySet.map(_.split("\t")(1)).distinct().count().toDouble

      val populT = entityCnt/totalTweets
      val populU = entityUserCnt/totalUserCnt

      val popularity = populT * populU

      all_values += (sdfDay.format(new Date(startTime )) + " - " + sdfDay.format(new Date(endTime )) -> popularity)
    }

    var export_topK = ""

    for (i <- 1 to topK) {
      val best = all_values.minBy(_._2)
      export_topK += best._1 + " : " + best._2 + "\n"
      all_values.remove(best._1)
    }

    println(export_topK)

  }
}

