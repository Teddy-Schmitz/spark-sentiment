/**
 * Created by teddy on 4/28/15.
 */


import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext._

object TwitterStream {

  def main(args :Array[String]): Unit ={

    System.setProperty("twitter4j.oauth.consumerKey", "")
    System.setProperty("twitter4j.oauth.consumerSecret", "")
    System.setProperty("twitter4j.oauth.accessToken", "")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "")

    val filters = Array("")

    val sc = new SparkConf().setAppName("TwitterPopularTags")
    val ssc = new StreamingContext(sc, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None)

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))


    // Print popular hashtags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
