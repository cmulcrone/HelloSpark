package com.idgenius.prototype.streaming

import org.apache.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by cmulcrone on 7/5/16.
  */
object Tutorial {
  def main(args: Array[String]): Unit = {
    //Setup context configuration
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

    //Create streaming context
    val ssc = new StreamingContext(conf, Seconds(1))

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("172.16.34.133", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
