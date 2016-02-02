package io.netpie.spark

import org.apache.log4j.{Logger, Level}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

/*
 * microgear.cache looks like this
 *
{"_":{"key":"60Q9dXFOySqDTOe","requesttoken":null,"accesstoken":{"token":"VfD3hmI
5xx9v9ZdU","secret":"WHDj3woLZ62kugEGqnyn5gtiR1oSDeQp","endpoint":"pie://gb.netpi
e.io:1883","revokecode":"ATvWovw8bdFHhZdnibXQFwngrvQ="}}}
 *
 */

object SparkNetpieDemo {

  def main(args: Array[String]) {

    Logger.getRootLogger.setLevel(Level.WARN)
    if (args.length < 7) {
       System.err.println("Usage: SparkNetpieDemo <brokerUrl> <appId> <appKey> <appSecret> <gearname> <token> <secret>")
       System.exit(1)
    }

    val Seq(brokerUrl, appId, appKey, appSecret, gearName, token, secret) = args.toSeq
    val sparkConf = new SparkConf().setAppName("SparkNetpieDemo")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val lines = new NetpieInputDStream(ssc, brokerUrl, appId, appKey, appSecret, gearName, token, secret, StorageLevel.MEMORY_ONLY_SER_2)

    val words = lines.flatMap(x => x.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
