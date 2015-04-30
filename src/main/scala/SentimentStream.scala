/**
 * Created by teddy on 4/28/15.
 */


import org.apache.spark.streaming.{Seconds, StreamingContext, Minutes}
import org.apache.spark.streaming.twitter._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.redis._


object SentimentStream {

  def main(args :Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    System.setProperty("twitter4j.oauth.consumerKey", "")
    System.setProperty("twitter4j.oauth.consumerSecret", "")
    System.setProperty("twitter4j.oauth.accessToken", "")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "")

    val filters = Array("rackspace", "raxspace", "Rackspace", "RACKSPACE")

    val sparkConf = new SparkConf().setAppName("TwitterSentimentStream")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))

    val pos_words = sc.textFile("file:///root/pos-words.txt")
    val neg_words = sc.textFile("file:///root/neg-words.txt")
    val stop_words = sc.textFile("file:///root/stop-words.txt").map((_,1))

    val stream = TwitterUtils.createStream(ssc, None,filters)

    val clients = new RedisClientPool("localhost", 6379)

    val raxTweet = stream.filter(_.getLang() == "en").map(_.getText().replaceAll("[^a-zA-Z\\s]", "")
      .trim().toLowerCase())

    val words = raxTweet.flatMap(_.split(" ")).map((_,1)).transform(_.leftOuterJoin(stop_words)).map({case (tweet, value) => tweet})

    val pos = words.transform(_.intersection(pos_words))

    val neg = words.transform(_.intersection(neg_words))

    val posLast60 = pos.map((_,1)).reduceByKeyAndWindow((a:Int,b:Int) => (a+b),Minutes(60),Minutes(1))
      .map{case (tweet, count) => (count, tweet)}
      .transform(_.sortByKey(false))

    val negLast60 = neg.map((_,1)).reduceByKeyAndWindow((a:Int,b:Int)=> (a+b),Minutes(60),Minutes(1))
      .map{case (tweet, count) => (count, tweet)}
      .transform(_.sortByKey(false))

    posLast60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPositive topics in last 60 minutes (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
      clients.withClient {
        client => {
          client.set("pos_comments", topList)
        }
      }})

    negLast60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nNegative topics in last 60 minutes (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
      clients.withClient {
        client => {
          client.set("neg_comments", topList)
        }
      }})

    val posCount = pos.window(Minutes(60), Minutes(1)).count()
    val negCount = neg.window(Minutes(60), Minutes(1)).count()

    posCount.foreachRDD(rdd => {
      val count = rdd.first
      println ("\nTotal positive comments: %s".format(count))
      clients.withClient {
        client => {
          client.set("pos_count", count)
        }
        }})
    negCount.foreachRDD(rdd => {
      val count = rdd.first
      println ("\nTotal negative comments: %s".format(count))
      clients.withClient {
        client => {
          client.set("neg_count", count)
        }
      }})

    ssc.start()
    ssc.awaitTermination()

  }

}
