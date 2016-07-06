package com.idgenius.prototype.streaming

import org.apache.spark._
import org.apache.spark.sql._


/**
  * Created by cmulcrone on 7/5/16.
  */
object SQLExploration {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SQL Exploration").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")

    val wikiData = sqlContext.read.parquet("data/wiki_parquet")

    System.out.println(wikiData.count())

    wikiData.registerTempTable("wikiData")

    val countResult = sqlContext.sql("SELECT COUNT(*) FROM wikiData").collect()

    val sqlCount = countResult.head.getLong(0)

    sqlContext.sql("SELECT username, COUNT(*) AS cnt FROM wikiData WHERE username <> '' GROUP BY username ORDER BY cnt DESC LIMIT 10").collect().foreach(println)

    sqlContext.sql("SELECT COUNT(*) FROM wikiData WHERE text like '%california%' ").collect().foreach(println)
  }
}