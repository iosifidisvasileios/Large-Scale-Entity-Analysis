package MeasureAggregators

import java.text.SimpleDateFormat
import java.util.Date

import SpamDetection.ProcessingOfRow
import breeze.linalg.{max, min}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Period}

import scala.util.Try

object TemporalAggregation {


  def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[DateTime]      =Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))

  def calculateCONtNew(e1: Double, e2: Double) : Double = {
    Try(e2/e1).getOrElse(0.0)
  }
  def calculateCONe(e1Entities: RDD[String], e2Entities: RDD[String]) : Double = {
    val e1EntitiesCnt = e1Entities.distinct().count.toDouble
    //    val e2EntitiesCnt = e2Entities.distinct().count.toDouble
    val e1ANDe2_Entities = e2Entities.distinct().intersection(e1Entities.distinct()).count.toDouble
    val CONe = e1ANDe2_Entities/(e1EntitiesCnt  - e1ANDe2_Entities)
    CONe
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

    val processingOfRow: ProcessingOfRow = new ProcessingOfRow
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM")

    def conf = new SparkConf().setAppName(SingleEntityMeasures.getClass.getName)

    val sc = new SparkContext(conf)
    val argsLen = args.length

    sc.setLogLevel("ERROR")

    val entity = args(0).toString
    val delta = args(1).toDouble
    val topK = args(2).toInt

    val startDateTemp = args(3)
    val endDateTemp = args(4)
    val inputDirectory = args(5).toString

    val startDate: Date = sdf.parse(startDateTemp)
    val endDate:   Date = sdf.parse(endDateTemp)
//    val startDate: Date = sdf.parse("2013-01")
//    val endDate: Date = sdf.parse("2013-12")
//
    val myList = dateRange(new DateTime(startDate), new DateTime(endDate), new Period().withMonths(1)).toList.map(_.toDate).map(x => sdf.format(x) ).distinct
    val popularityList = scala.collection.mutable.MutableList[String]()
    popularityList += "date, popularity_tweets, popularity_users, popularity, attitude, sentimentality, controversiality"

    myList.foreach{ step=>
      println(step)

      val indexedRdd = sc.textFile(args(argsLen - 1) + step)
      val entitySet = indexedRdd.filter(_.split("\t")(3).contains(entity)).cache

      val totalTweets = indexedRdd.count().toDouble
      val totalUserCnt = indexedRdd.map(_.split("\t")(1)).distinct().count().toDouble

      val entityCnt = entitySet.count().toDouble
      val entityUserCnt = entitySet.map(_.split("\t")(1)).distinct().count().toDouble

      val populT = entityCnt/totalTweets
      val populU = entityUserCnt/totalUserCnt

      val popularity = populT * populU

      val attitudePosCnt = entitySet.map(_.split("\t")(4).split(" ")(0).toInt).sum()
      val attitudeNegCnt = entitySet.map(_.split("\t")(4).split(" ")(1).toInt).sum()
      val sentimentalityCnt = attitudePosCnt - attitudeNegCnt - (2*entityCnt)

      val attitude = (attitudePosCnt + attitudeNegCnt)/entityCnt
      val sentimentality = sentimentalityCnt/entityCnt

      val controversiality = controversialityForOne(entitySet, entityCnt, delta)


/*
        val myList = List(
        ("popul_Tweets", populT ),
        ("popul_Users",populU ),
        ("popularity", popularity),
        ("attitude", attitude),
        ("sentimentality", sentimentality),
        ("controversiality", controversiality)
      )
      sc.parallelize(myList).repartition(1).saveAsTextFile("TPDL_Examples/popularity_" + entity + "_" + step)
*/
      val csvString =  step + "," + populT + "," + populU + "," + popularity + "," + attitude + "," + sentimentality + "," + controversiality
      popularityList += csvString



     val entitySetALL = entitySet.map(s => s.split("\t")(3)).flatMap(t => t.split(";"))
        .map(u => u.split(":")(1)).cache()
        .filter( x => !processingOfRow.isStopword(x) && !x.equals(entity))

      val e1Cnt = entitySetALL.count().toDouble

      val entitySetCnt = sc.parallelize(entitySetALL.countByValue().toSeq)
        .sortBy(k => k._2, ascending = false)
        .take(topK)

      val approvedEntities = sc.parallelize(entitySetCnt.map{x=>
        (x._1, calculateCONtNew(e1Cnt, x._2.toDouble))
      })

      approvedEntities.repartition(1).saveAsTextFile("TPDL_Examples/K-Network_" + entity + "_" + step)

    }

    sc.parallelize(popularityList).repartition(1).saveAsTextFile("TPDL_Examples/popularity_" + entity )
  }
}

