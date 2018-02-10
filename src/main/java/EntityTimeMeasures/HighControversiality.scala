package EntityTimeMeasures

import java.text.SimpleDateFormat
import java.util.Date

import MeasureAggregators.SingleEntityMeasures
import breeze.linalg.{max, min}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Period}

import scala.collection.mutable

object HighControversiality {


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

  def controversialityForOne(entitySet: RDD[String], entityCount: Double, thresholdDelta : Double) : Double = {
    val posCnt = entitySet.filter { x =>
      x.split("\t")(4).split(" ")(0).toInt + x.split("\t")(4).split(" ")(1).toInt >= thresholdDelta
    }.count().toDouble

    val negCnt = entitySet.filter { x =>
      x.split("\t")(4).split(" ")(0).toInt + x.split("\t")(4).split(" ")(1).toInt <= -thresholdDelta
    }.count().toDouble

    (min(posCnt, negCnt) / max(posCnt, negCnt)) * ((posCnt + negCnt)/entityCount)
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
    val delta = args(5).toDouble
    val inputDirectory = args(6).toString

    val startDate: Date = sdfDay.parse(startDateTemp)
    val endDate:   Date = sdfDay.parse(endDateTemp)


    val myListPerDayGran = aggregateNextDate(new DateTime(startDate).minusDays(1), new DateTime(endDate), granularity)

    for (index <- myListPerDayGran.indices)
    {
      val startTime =  myListPerDayGran(index).plusDays(1).getMillis
      val endTime =  myListPerDayGran(index + 1).getMillis

      val indexedRdd = sc.textFile(inputDirectory).filter{x=>
        val temp: Date = sdf3.parse(x.split("\t")(2))
        temp.getTime >= startTime && temp.getTime <= endTime
      }.cache

      val entitySet = indexedRdd.filter(_.split("\t")(3).contains(entity)).cache

      val entityCnt = entitySet.count().toDouble

      val controversiality = controversialityForOne(entitySet, entityCnt, delta)


      all_values += (sdfDay.format(new DateTime(myListPerDayGran(index)).plusDays(1)) + " - " + sdfDay.format(new DateTime(myListPerDayGran(index + 1))) -> controversiality)
    }

    var export_topK = ""

    for (i <- 1 to topK) {
      val best = all_values.maxBy(_._2)
      export_topK += best._1 + " : " + best._2
      all_values.remove(best._1)
    }

    println(export_topK)

  }
}

