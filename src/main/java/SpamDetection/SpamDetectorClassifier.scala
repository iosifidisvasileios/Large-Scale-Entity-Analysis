package SpamDetection

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.Logger
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Period}




object SpamDetectorClassifier {

  def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[DateTime]      =Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))

  def main(args: Array[String]) {
    val from = new DateTime().withYear(2013).withMonthOfYear(1).withDayOfMonth(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0)
    val to = new DateTime().withYear(2017).withMonthOfYear(3).withDayOfMonth(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0)

    def conf = new SparkConf().setAppName(SpamDetectorClassifier.getClass.getName)
    val processingOfRow: ProcessingOfRow = new ProcessingOfRow
    val sc = new SparkContext(conf)
    val sdf: SimpleDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy")
    val sdf_2: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    println("Hello, world!")
    sc.setLogLevel("ERROR")

    val htf = new HashingTF(1500000)
    val trainingInput = sc.textFile(args(0))
    val unlabeledInput = sc.textFile(args(1))



    val trainingSet = trainingInput.map { line =>
      val parts = line.split(',')
      var preprocessed = parts(2)

      preprocessed = processingOfRow.slangProcess(preprocessed.toLowerCase)
      preprocessed = processingOfRow.clearText(preprocessed)
      preprocessed = processingOfRow.clearWhitespace(preprocessed)
      //lowercase words, remove repetitions, replace slang terms
      preprocessed = processingOfRow.slangProcess(preprocessed)
      preprocessed = processingOfRow.clearWhitespace(preprocessed)
      preprocessed = processingOfRow.negationsBasedOnVerbs(preprocessed)
      preprocessed = processingOfRow.clearWhitespace(preprocessed)
      preprocessed = processingOfRow.negationsBasedOnAdj(preprocessed)
      preprocessed = processingOfRow.clearWhitespace(preprocessed)
      //remove emoticons, non ascii chars, symbols
      preprocessed = processingOfRow.removeNonAscii(preprocessed)
      preprocessed = processingOfRow.clearWhitespace(preprocessed)
      preprocessed = processingOfRow.removeStopwords(preprocessed)
      preprocessed = processingOfRow.clearWhitespace(preprocessed)

      var id= 0
      if (parts(1).equals("ham")){
        id = 1
      }
      LabeledPoint(id.toDouble, htf.transform(preprocessed.split(' ')))
    }

    val unlabeledMapped = unlabeledInput.map { line =>
      val parts = line.split('\t')
      var preprocessed = parts(7)

      preprocessed = processingOfRow.slangProcess(preprocessed.toLowerCase)
      preprocessed = processingOfRow.clearText(preprocessed)
      preprocessed = processingOfRow.clearWhitespace(preprocessed)
      //lowercase words, remove repetitions, replace slang terms
      preprocessed = processingOfRow.slangProcess(preprocessed)
      preprocessed = processingOfRow.clearWhitespace(preprocessed)
      preprocessed = processingOfRow.negationsBasedOnVerbs(preprocessed)
      preprocessed = processingOfRow.clearWhitespace(preprocessed)
      preprocessed = processingOfRow.negationsBasedOnAdj(preprocessed)
      preprocessed = processingOfRow.clearWhitespace(preprocessed)
      //remove emoticons, non ascii chars, symbols
      preprocessed = processingOfRow.removeNonAscii(preprocessed)
      preprocessed = processingOfRow.clearWhitespace(preprocessed)
      preprocessed = processingOfRow.removeStopwords(preprocessed)
      preprocessed = processingOfRow.clearWhitespace(preprocessed)

      (line, htf.transform(preprocessed.split(' ')))
    }

    var spamTraining = trainingSet.filter(_.label == 0)
    val hamTraining = trainingSet.filter(_.label == 1)

    val splits = hamTraining.randomSplit(Array(0.23, 0.77))
    spamTraining = spamTraining.union(splits(0)).cache()

    val model = NaiveBayes.train(spamTraining , lambda = 1.0,  modelType = "multinomial")

    println("spam counts: " + unlabeledMapped.filter{item =>
      model.predict(item._2) == 0
    }.count())

    val labeledHam = unlabeledMapped.filter{item =>
          model.predict(item._2) == 1
          }.map(_._1).cache()
    /*
        println("labeledSpam labeled: "+ labeledSpam.count )
        println("labeledHam labeled: "+ labeledHam.count )

        labeledHam.saveAsTextFile("labeled_Ham")
    */

    dateRange(from, to, new Period().withDays(1)).toList.foreach { step =>
      println(step)
      val unlabeledMapped = labeledHam.filter{ line =>
        val parts = line.split('\t')
        val stringDate = parts(2)
        val temp: Date = sdf.parse(stringDate)
        try {

          val date: DateTime = new DateTime(temp)

          step.getMonthOfYear.equals(date.getMonthOfYear) && step.getYear.equals(date.getYear) && step.getDayOfMonth.equals(date.getDayOfMonth)

        } catch {
          case e: java.lang.NumberFormatException => println(line, e)
            false
        }
      }

      if (!unlabeledMapped.isEmpty()){
        unlabeledMapped.saveAsTextFile("Indexed/" + sdf_2.format(step.toDate))
      }
    }



  }

}
