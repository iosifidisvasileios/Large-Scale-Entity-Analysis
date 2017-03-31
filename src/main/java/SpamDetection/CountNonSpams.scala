package SpamDetection

import java.text.SimpleDateFormat
import java.util.Date

import breeze.linalg.{max, min}
import breeze.numerics.abs
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Period}

object CountNonSpams {

  def main(args: Array[String]) {
    def conf = new SparkConf().setAppName(CountNonSpams.getClass.getName)

    val sc = new SparkContext(conf)
    //    println("Hello, world!")
    sc.setLogLevel("ERROR")

    val indexedRdd = sc.textFile(args(0))
    val myRdd = indexedRdd.map(x=> x.split("\t")(0)).cache()
    println("myRdd.count() " + myRdd.count())
    println("myRdd.distinct.count()" +myRdd.distinct().count())
  }

}
