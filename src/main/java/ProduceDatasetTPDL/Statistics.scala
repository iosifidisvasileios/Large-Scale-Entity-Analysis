package ProduceDatasetTPDL

import java.security.MessageDigest
import java.text.SimpleDateFormat

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Period}

object Statistics {
  def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[DateTime]      =Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))

  def main(args: Array[String]) {

    val from = new DateTime().withYear(2013).withMonthOfYear(1).withDayOfMonth(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0)
    val to = new DateTime().withYear(2017).withMonthOfYear(1).withDayOfMonth(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0)
    val sdf_2: SimpleDateFormat = new SimpleDateFormat("yyyy-MM")


    def conf = new SparkConf().setAppName(Statistics.getClass.getName)

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    println("date,tweets,entities_all, entities_distinct, no_entities")
    dateRange(from, to, new Period().withMonths(1)).toList.foreach { step =>
//      println("Indexed/" + sdf_2.format(step.toDate))
      val initialVersion = sc.textFile("Indexed/" + sdf_2.format(step.toDate))
      val entities = initialVersion.map(s => s.split("\t")(3)).filter(x=> !x.equals("null;")).flatMap(t => t.split(";")).map(u => u.split(":")(1)).cache

      val total_tweets= initialVersion.count()
      val total_entities = entities.count()
      val distinct_entities = entities.distinct().count()

      val null_entities = initialVersion.map(s => s.split("\t")(3)).filter(x=> x.equals("null;")).count()

      println(sdf_2.format(step.toDate) + ", " + total_tweets + "," + total_entities + "," + distinct_entities + "," + null_entities)

    }
  }
}


