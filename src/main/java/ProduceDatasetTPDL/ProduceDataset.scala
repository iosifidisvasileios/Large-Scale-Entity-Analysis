package ProduceDatasetTPDL

import java.security.MessageDigest
import java.text.SimpleDateFormat

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Period}

object ProduceDataset {
  def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[DateTime]      =Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))

  def concat(ss: String*) = ss filter (_.nonEmpty) mkString "\t"

  def md5(s: String) = {
    MessageDigest.getInstance("MD5").digest(s.getBytes).toString
  }
  def main(args: Array[String]) {

    val from = new DateTime().withYear(2013).withMonthOfYear(1).withDayOfMonth(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0)
    val to = new DateTime().withYear(2017).withMonthOfYear(1).withDayOfMonth(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0)
    val sdf_2: SimpleDateFormat = new SimpleDateFormat("yyyy-MM")


    def conf = new SparkConf().setAppName(ProduceDataset.getClass.getName)

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    dateRange(from, to, new Period().withMonths(1)).toList.foreach { step =>
      println("Indexed/" + sdf_2.format(step.toDate))
      val initialVersion = sc.textFile("Indexed/" + sdf_2.format(step.toDate))
      initialVersion.map { line =>
        val stringArray = line.split("\t")
        concat(stringArray(0), md5(stringArray(1)), stringArray(2), stringArray(3), stringArray(4))
      }.saveAsTextFile("TPDL_Dataset/" + sdf_2.format(step.toDate), classOf[GzipCodec])
    }
  }
}


