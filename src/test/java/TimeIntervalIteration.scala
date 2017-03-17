import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.Logger
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Days, LocalDate, Period}

object TimeIntervalIteration {

  //   def conf = new SparkConf().setAppName("App_Name").setMaster("spark://localhost:6066").set("spark.ui.port","8080");
  //   var sc: SparkContext = _


  private[this] val logger = Logger.getLogger(getClass().getName());
  def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[DateTime]      =Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))


  def main(args: Array[String]) {
    println("Hello, world!")
    val sdf: SimpleDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy")
    val sdf_2: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val from = new DateTime().withYear(2013).withMonthOfYear(1).withDayOfMonth(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0)
    val to = new DateTime().withYear(2017).withMonthOfYear(2).withDayOfMonth(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0)
    val temp: Date = sdf.parse("Mon Aug 03 02:29:44 +0000 2013")
    val date2 : DateTime = new DateTime(temp)


    dateRange(from, to, new Period().withMonths(1)).toList.foreach(step =>
    if(step.getMonthOfYear.equals(date2.getMonthOfYear) && step.getYear.equals(date2.getYear))
      println(step)
    )


//    println(sdf_2.format(temp).split("-")(0))
//    println(sdf_2.format(temp).split("-")(1))
//    println(sdf_2.format(date2.plusMonths(1).toDate).split("-")(1))
//

    }

  }


