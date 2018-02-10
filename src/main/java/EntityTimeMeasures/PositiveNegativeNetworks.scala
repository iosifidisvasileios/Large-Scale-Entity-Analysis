package EntityTimeMeasures

import java.text.SimpleDateFormat
import java.util.Date

import MeasureAggregators.SingleEntityMeasures
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Period}

import scala.util.Try

object PositiveNegativeNetworks {


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
  def calculateCONtNew(e1: Double, e2: Double) : Double = {
    Try(e2/e1).getOrElse(0.0)
  }

  def calculateEplusEminus(tweetSet: RDD[String], entities: RDD[String], entityCooccurred: Double, threshold: Double, topK: Int) : (Array[(String,  Double)], Array[(String, Double)])   = {
    val tempTweets = tweetSet.collect()
    val entitiesUponThreshold = entities.map{ x=>
      val rddUnion = tempTweets.filter(_.split("\t")(3).contains(x))
      val co_occurrence = rddUnion.length.toDouble

      val posCnt = rddUnion.map{ y =>
        y.split("\t")(4).split(" ")(0).toInt + y.split("\t")(4).split(" ")(1).toDouble
      }.sum

      val negCnt = rddUnion.map{ y =>
        y.split("\t")(4).split(" ")(0).toInt + y.split("\t")(4).split(" ")(1).toDouble
      }.sum

      val accepted = (posCnt + negCnt)/co_occurrence
      (x, accepted, co_occurrence)
    }.filter{ y=> Math.abs(y._2) >= threshold }.distinct.cache()

    val PosNetwork = entitiesUponThreshold.filter(_._2 >= threshold).map{x=>
      (x._1, calculateCONtNew(entityCooccurred, x._3))
    }.sortBy(K => K._2, ascending = false).take(topK)

    val NegNetwork = entitiesUponThreshold.filter(_._2 <= -threshold).map{x=>
      (x._1, calculateCONtNew(entityCooccurred, x._3))
    }.sortBy(K => K._2, ascending = false).take(topK)

    (PosNetwork, NegNetwork)
  }

  def main(args: Array[String]) {

    val sdfDay: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val sdf3: SimpleDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy")

    def conf = new SparkConf().setAppName(SingleEntityMeasures.getClass.getName)

    val sc = new SparkContext(conf)
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
      val e1Cnt = entitySet.count().toDouble

      val tweetSet = indexedRdd.filter{ line=>
        var flag = false
        val entity = line.split("\t")(3)

        if (!entity.equals("null;")){
          val eList = entity.split(";")
          for(name <- eList) {
            if (name.split(":")(1).equals(entity)){
              flag = true
            }
          }
        }
        flag
      }.cache()

      val E = calculateEplusEminus(tweetSet, entitySet, e1Cnt, delta, topK)
      sc.parallelize(E._1).repartition(1).saveAsTextFile("EplusNetwork_"  + entity + "_" + args(0) + "_" + args(1))
      sc.parallelize(E._2).repartition(1).saveAsTextFile("EminusNetwork_" + entity + "_" + args(0) + "_" + args(1))
    }

  }
}

