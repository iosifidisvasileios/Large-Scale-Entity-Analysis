package MeasureAggregators

import java.text.SimpleDateFormat
import java.util.Date

import SpamDetection.ProcessingOfRow
import breeze.linalg.{max, min}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Period}

import scala.collection.mutable.HashSet
import scala.io.Source
import scala.util.Try

object ListEntityMeasures {


  def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[DateTime]      =Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))

  def calculateCONtNew(e1: Double, e2: Double) : Double = {
    Try(e2/e1).getOrElse(0.0)
  }
  def calculateCONe(e1Entities: RDD[String], e2Entities: RDD[String]) : Double = {
    val e1EntitiesCnt = e1Entities.distinct().count.toDouble
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
    //    val t0 = System.nanoTime()

    val processingOfRow: ProcessingOfRow = new ProcessingOfRow
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM")
    val sdf2: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val sdf3: SimpleDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy")
    var entitiesList = HashSet[String]()
    def conf = new SparkConf().setAppName(this.getClass.getName)

    val sc = new SparkContext(conf)
    val argsLen = args.length
    sc.setLogLevel("ERROR")
    if (argsLen != 6){
      println("invalid number of arguments, must set up to 6. exiting...")
      exit(1)
    }
    var startingYear = ""
    var startingMonth = ""
    var startingDay = ""
    var endingYear = ""
    var endingMonth = ""
    var endingDay = ""

    var flagLoop = true
    while (flagLoop) {
      //      println("give starting date: \"year-month\". example: 2013-01-01")
      //      val buffer = Console.readLine
      val buffer = args(0).toString
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

        if (!flag) {
          flagLoop = false
        }
        else{
          sys.exit(1)
        }

      }else{
        println("wrong type of date")
        sys.exit(1)
      }
    }

    val startDate: Date = sdf.parse(startingYear + "-" + startingMonth)
    val startDateFull: Date = sdf2.parse(startingYear + "-" + startingMonth + "-" + startingDay)
    flagLoop = true
    while (flagLoop) {
      val buffer = args(1).toString
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
        if (!flag) {
          flagLoop = false
        }
        else{
          sys.exit(1)
        }

      }else{
        println("wrong type of date")
        sys.exit(1)
      }

    }

    val endDate: Date = sdf.parse(endingYear + "-" + endingMonth)
    val endDateFull: Date = sdf2.parse(endingYear + "-" + endingMonth + "-" + endingDay)

    val myList = dateRange(new DateTime(startDate), new DateTime(endDate), new Period().withMonths(1)).toList.map(_.toDate).map(x => sdf.format(x) ).distinct
    //    val listConcat = myList.mkString(",")

    val listSource =  Source.fromFile(args(2))
    for(line <- listSource.getLines() ) {
      entitiesList.add(line.replace("\n", ""))
    }

    val delta = args(3).toDouble
    val topK = args(4).toInt

    println(entitiesList)

    myList.foreach{ step=>

      println(step)
      val indexedRdd = sc.textFile(args(argsLen - 1) + step).filter{x=>
        val temp: Date = sdf3.parse(x.split("\t")(2))
        temp.getTime >= startDateFull.getTime && temp.getTime <= endDateFull.getTime
      }.cache

      val totalTweets = indexedRdd.count().toDouble
      val totalUserCnt = indexedRdd.map(_.split("\t")(1)).distinct().count().toDouble

      println("totalTweets " +totalTweets )

      val totalEntitySet = indexedRdd.filter{ line=>
        var flag = false
        val entity = line.split("\t")(3)

        if (!entity.equals("null;")){
          val eList = entity.split(";")
          for(name <- eList) {
            if (entitiesList.contains(name.split(":")(1))){
              flag = true
            }
          }
        }
        flag
      }.cache()

      for(item <- entitiesList) {
        //      val entitySet = totalEntitySet.filter(_.split("\t")(3).contains(item)).cache()

        val entitySet = indexedRdd.filter { line =>
          var flag = false
          val entity = line.split("\t")(3)

          if (!entity.equals("null;")) {
            val eList = entity.split(";")
            for (name <- eList) {
              if (name.split(":")(1).equals(item)) {
                flag = true
              }
            }
          }
          flag
        }.cache()

        val entityCnt = entitySet.count().toDouble
        println(item + " entityCnt " + entityCnt)

        val entityUserCnt = entitySet.map(_.split("\t")(1)).distinct().count().toDouble


        val populT = entityCnt / totalTweets
        val populU = entityUserCnt / totalUserCnt

        val popularity = populT * populU

        val attitudePosCnt = entitySet.map(_.split("\t")(4).split(" ")(0).toInt).sum()
        val attitudeNegCnt = entitySet.map(_.split("\t")(4).split(" ")(1).toInt).sum()
        val sentimentalityCnt = attitudePosCnt - attitudeNegCnt - (2 * entityCnt)

        val attitude = (attitudePosCnt + attitudeNegCnt) / entityCnt
        val sentimentality = sentimentalityCnt / entityCnt

        val controversiality = controversialityForOne(entitySet, entityCnt, delta)

        val myListTemp = List(
          ("popul_Tweets", populT),
          ("popul_Users", populU),
          ("popularity", popularity),
          ("attitude", attitude),
          ("sentimentality", sentimentality),
          ("controversiality", controversiality)
        )

        sc.parallelize(myListTemp).repartition(1).saveAsTextFile("ExtractedEntities/" + item + "/popularity_" + step)

        val entitySetForTopK = entitySet.map(s => s.split("\t")(3)).flatMap(t => t.split(";"))
          .map(u => u.split(":")(1)).cache()
          .filter(x => !processingOfRow.isStopword(x) && !x.equals(item))

        ////////
        val e1Cnt = entitySetForTopK.count().toDouble
        val entitySetCnt = sc.parallelize(entitySetForTopK.countByValue().toSeq)
          .sortBy(k => k._2, ascending = false)
          .take(topK)

        val approvedEntities = sc.parallelize(entitySetCnt.map { x =>
          (x._1, calculateCONtNew(e1Cnt, x._2.toDouble))
        })
        approvedEntities.repartition(1).saveAsTextFile("ExtractedEntities/" + item + "/K-Network_" + step)

        val E = calculateEplusEminus(entitySet, entitySetForTopK, e1Cnt, delta, topK)
        sc.parallelize(E._1).repartition(1).saveAsTextFile("ExtractedEntities/" +item + "/EplusNetwork_" + step)
        sc.parallelize(E._2).repartition(1).saveAsTextFile("ExtractedEntities/" +item + "/EminusNetwork_"+ step)

      }
    }
    //    println("Elapsed time: " + (System.nanoTime() - t0) + "ns")

  }

}

