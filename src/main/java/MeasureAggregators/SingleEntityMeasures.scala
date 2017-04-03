package MeasureAggregators

import java.text.SimpleDateFormat
import java.util.Date

import SpamDetection.ProcessingOfRow
import breeze.linalg.{max, min}
import breeze.numerics.abs
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Period}

import scala.util.Try

object SingleEntityMeasures {


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

  /*
  def flatten(ls: List[Any]): List[Any] = ls flatMap {
    case i: List[_] => flatten(i)
    case e => List(e)
  }


 def calculateCONh(e1HashTags: RDD[String], e2HashTags: RDD[String]) : Double = {
     val e1HashTagsCnt = e1HashTags.distinct().count.toDouble
     //    val e2HashTagsCnt = e2HashTags.distinct().count.toDouble
     val e1ANDe2_HashTags = e2HashTags.distinct().intersection(e1HashTags.distinct()).count.toDouble
     val CONh = e1ANDe2_HashTags/(e1HashTagsCnt - e1ANDe2_HashTags)

     return CONh
   }
  def calculateCONm(e1Mensions: RDD[String], e2Mensions: RDD[String]) : Double = {
     val e1MensionsCnt = e1Mensions.distinct().count.toDouble
     //    val e2MensionsCnt = e2Mensions.distinct().count.toDouble
     val e1ANDe2_Mensions = e2Mensions.distinct().intersection(e1Mensions.distinct()).count.toDouble
     val CONm = e1ANDe2_Mensions/(e1MensionsCnt - e1ANDe2_Mensions)

     return CONm
   }
   */

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
    var startingYear = ""
    var startingMonth = ""
    var endingYear = ""
    var endingMonth = ""

    var flagLoop = true
    while (flagLoop) {
      println("give starting date: \"year-month\". example: 2013-01")
      //      val buffer = Console.readLine
      val buffer = args(0).toString
      if (buffer.split("-").length == 2) {
        startingYear = buffer.split("-")(0)
        startingMonth = buffer.split("-")(1)
        var flag = false

        if (!startingYear.forall(_.isDigit)) {
          println("wrong year")
          flag = true
        }
        if (!startingMonth.forall(_.isDigit)) {
          println("wrong month")
          flag = true
        }
        if (!flag) flagLoop = false
      }
    }

    val startDate: Date = sdf.parse(startingYear + "-" + startingMonth)

    flagLoop = true
    while (flagLoop) {
      println("give ending date: \"year-month\". example: 2013-12")
      //      val buffer = Console.readLine
      val buffer = args(1).toString
      if (buffer.split("-").length == 2) {
        endingYear = buffer.split("-")(0)
        endingMonth = buffer.split("-")(1)
        var flag = false
        if (!endingYear.forall(_.isDigit)) {
          println("wrong year")
          flag = true
        }
        if (!endingMonth.forall(_.isDigit)) {
          println("wrong month")
          flag = true
        }
        if (!flag) flagLoop = false
      }
    }

    val endDate: Date = sdf.parse(endingYear + "-" + endingMonth)
    val myList = dateRange(new DateTime(startDate), new DateTime(endDate), new Period().withMonths(1)).toList.map(_.toDate).map(x => args(argsLen - 1) + sdf.format(x) ).distinct
    val listConcat = myList.mkString(",")

    println(listConcat)
    var bufferSelection =""
    flagLoop = true
    while (flagLoop) {
      println("Select 1 or 2 or 3\n1) Measures for an Entity\n2) Correlation Measures between 2 Entities\n3) Entity k-Network")
      //      bufferSelection = Console.readLine
      bufferSelection = args(2).toString
      if (bufferSelection.forall(_.isDigit) && bufferSelection.equals("1") || bufferSelection.equals("2") || bufferSelection.equals("3") )
        flagLoop = false
    }

    bufferSelection match {
      case "1" =>
        println("Give entity for search (exactly as it is written in Wikipedia):")
        //        val e1 = Console.readLine
        val e1 = args(3).toString
        println(e1)

        val delta = args(4).toDouble
//        val percentageForControversiality = args(5).toDouble

        val indexedRdd = sc.textFile(listConcat)
        val totalTweets = indexedRdd.count().toDouble
        val totalUserCnt = indexedRdd.map(_.split("\t")(1)).distinct().count().toDouble
        val entitySet = indexedRdd.filter(_.split("\t")(3).contains(e1))

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

        val myList = List(
          ("popul_Tweets", populT ),
          ("popul_Users",populU ),
          ("popularity", popularity),
          ("attitude", attitude),
          ("sentimentality", sentimentality),
          ("controversiality", controversiality)
        )

        sc.parallelize(myList).repartition(1).saveAsTextFile("popularity_" + e1 + "_" + args(0) + "_" + args(1))

      case "2" =>
        println("Give first  entity for search (exactly as it is written in Wikipedia):")
        val e1 = args(3).toString
        println(e1)
        //        val e1 = Console.readLine
        println("Give second entity for search (exactly as it is written in Wikipedia):")
        val e2 = args(4).toString
        println(e2)
        //        val e2 = Console.readLine

        val indexedRdd = sc.textFile(listConcat)
        val entitySetE1 = indexedRdd.filter(_.split("\t")(3).contains(e1)).cache
        val entitySetE2 = indexedRdd.filter(_.split("\t")(3).contains(e2)).cache

        val entitySet = entitySetE1.map(s => s.split("\t")(3)).flatMap(t => t.split(";")).map(u => u.split(":")(1)).cache
        val entityMap= entitySet.countByValue()
        //        sc.parallelize(entityMap.toSeq).saveAsTextFile("myEntities")
        var CONt = 0.0
        if(entityMap.contains(e2)) {
          CONt = calculateCONtNew(entitySet.count().toDouble, entityMap(e2).toDouble)
        }

        println("Metric CONt : " + CONt)

        val CONe = calculateCONe(entitySetE1.map(s => s.split("\t")(3)).flatMap(t => t.split(";")).map(u => u.split(":")(1)).distinct(),
          entitySetE2.map(s => s.split("\t")(3)).flatMap(t => t.split(";")).map(u => u.split(":")(1)).distinct())
        println("Metric CONe : " + CONe)

        //        val CONh = calculateCONh(entitySetE1.map(s => s.split("\t")(6)).flatMap(t => t.split(" ")).map(u => u.toLowerCase).distinct(),
        //          entitySetE2.map(s => s.split("\t")(6)).flatMap(t => t.split(" ")).map(u => u.toLowerCase).distinct())
        //        println("Metric CONh : " + CONh)
        //
        //        val CONm = calculateCONm(entitySetE1.map(s => s.split("\t")(5)).flatMap(t => t.split(" ")).map(u => u.toLowerCase).distinct(),
        //          entitySetE2.map(s => s.split("\t")(5)).flatMap(t => t.split(" ")).map(u => u.toLowerCase).distinct())
        //        println("Metric CONm : " + CONm)

        //        val CONtotal = (CONe + CONh +CONm + CONt) / 4
        val CONtotal = (CONe  + CONt) / 2
        //        println("TOTAL CONNECTION SCORE: " + CONtotal)

        val myList = List(
          ("CONt", CONt ),
          ("CONe",CONe ),
          /*
          ("CONh", CONh),
          ("CONm", CONm),
          */
          ("CONtotal", CONtotal))
        sc.parallelize(myList).repartition(1).saveAsTextFile("K-measures_" + e1 + "_" + e2 + "_" + args(0) + "_" + args(1))

      case "3" =>
        println("Give entity for search (exactly as it is written in Wikipedia):")
        val e1 = args(3).toString
        val delta = args(4).toDouble
        val topK = args(5).toInt
        val indexedRdd = sc.textFile(listConcat).cache()
        val tweetSet = indexedRdd.filter(_.split("\t")(3).contains(e1)).cache

        val entitySet = tweetSet.map(s => s.split("\t")(3)).flatMap(t => t.split(";"))
          .map(u => u.split(":")(1)).cache()
          .filter( x => !processingOfRow.isStopword(x) && !x.equals(e1))

        val e1Cnt = entitySet.count().toDouble
        val entitySetCnt = sc.parallelize(entitySet.countByValue().toSeq)
          .sortBy(k => k._2, ascending = false)
          .take(topK)

        val approvedEntities = sc.parallelize(entitySetCnt.map{x=>
          (x._1, calculateCONtNew(e1Cnt, x._2.toDouble))
        })
        approvedEntities.repartition(1).saveAsTextFile("K-Network_" + e1 + "_" + args(0) + "_" + args(1))

        val E = calculateEplusEminus(tweetSet, entitySet, e1Cnt, delta, topK)
        sc.parallelize(E._1).repartition(1).saveAsTextFile("EplusNetwork_"  + e1 + "_" + args(0) + "_" + args(1))
        sc.parallelize(E._2).repartition(1).saveAsTextFile("EminusNetwork_" + e1 + "_" + args(0) + "_" + args(1))

      /*
        case "4" =>
        println("Give entity for search (exactly as it is written in Wikipedia):")
        val e1 = args(3).toString
        val indexedRdd = sc.textFile(listConcat).cache()
        val tweetSet = indexedRdd.filter(_.split("\t")(3).contains(e1)).cache

        val entitySet = tweetSet.map(s => s.split("\t")(3)).flatMap(t => t.split(";")).map(u => u.split(":")(1)).cache

        val entitySetCnt = sc.parallelize(entitySet.countByValue().toSeq).filter( x => !processingOfRow.isStopword(x._1) && !x._1.equals(e1) )
        val countOfEntities = entitySetCnt.count().toDouble

        val approvedEntities = entitySetCnt.map(x=> (x._1, calculateCONtNew(countOfEntities, x._2.toDouble))).sortBy(K=> K._2, ascending = false).take(args(4).toInt)

        val hashTagSet = tweetSet.map(s => s.split("\t")(6)).flatMap(t => t.split(" ")).map(u => u.toLowerCase).distinct().cache
        val mensionSet = tweetSet.map(s => s.split("\t")(5)).flatMap(t => t.split(" ")).map(u => u.toLowerCase).distinct().cache

        val scores = approvedEntities.map{ entity=>

          println("this is the co_occured entity : ''' " + entity + " '''")

          val corelatedEntities = indexedRdd.filter(_.split("\t")(3).contains(entity._1)).map(s => s.split("\t")(3)).flatMap(t => t.split(";")).map(u => u.split(":")(1))

          val entitySetCnt_1 = sc.parallelize(corelatedEntities.countByValue().toSeq).filter( x =>
            !processingOfRow.isStopword(x._1) && !x._1.equals(e1) && !x._1.equals(entity._1))

          val approvedEntitiesTemp = entitySetCnt_1.sortBy(K=> K._2, ascending = false).take(args(5).toInt)

          val scoresTemp = approvedEntitiesTemp.map { x =>
            println("Co-Related Entity\t:" + x)
            val entitySetTemp_2 = indexedRdd.filter(_.split("\t")(3).contains(x._1)).cache

            val CONt = calculateCONtEplusEminus(tweetSet, entitySetTemp_2, e1, x._1)
            val CONe = calculateCONe(entitySet, entitySetTemp_2.map(s => s.split("\t")(3)).flatMap(t => t.split(";")).map(u => u.split(":")(1)).distinct())
            val CONm = calculateCONm(mensionSet, entitySetTemp_2.map(s => s.split("\t")(5)).flatMap(t => t.split(" ")).map(u => u.toLowerCase).distinct())
            val CONh = calculateCONh(hashTagSet, entitySetTemp_2.map(s => s.split("\t")(6)).flatMap(t => t.split(" ")).map(u => u.toLowerCase).distinct())
            val CONtotal = (CONe + CONh + CONm + CONt.head) / 4

            (x._1, CONtotal, CONt(1), CONt(2))
          }
          scoresTemp.toList
        }.toList

        sc.parallelize(flatten(scores).distinct).saveAsTextFile("entityScores")
        */
    }
  }
}

