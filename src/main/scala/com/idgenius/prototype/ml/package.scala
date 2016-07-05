package com.idgenius.prototype.ml

// General purpose library
import org.apache.spark.{SparkConf, SparkContext}

import scala.xml._

// Spark data manipulation libraries
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// Spark machine learning libraries
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.Pipeline

/**
  * Created by cmulcrone on 7/5/16.
  */
package object MachineLearningHelloWorld {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    val fileName = "Posts.small.xml"
    val textFile = sc.textFile(fileName)
    val postsXml = textFile.map(_.trim).
      filter(!_.startsWith("<?xml version=")).
      filter(_ != "<posts>").
      filter(_ != "</posts>")


    val postsRDD = postsXml.map { s =>
      val xml = XML.loadString(s)

      val id = (xml \ "@Id").text
      val tags = (xml \ "@Tags").text

      val title = (xml \ "@Title").text
      val body = (xml \ "@Body").text
      val bodyPlain = ("<\\S+>".r).replaceAllIn(body, " ")
      val text = (title + " " + bodyPlain).replaceAll("\n",
        " ").replaceAll("( )+", " ");

      Row(id, tags, text)
    }

    val schemaString = "Id Tags Text"
    val schema = StructType(
      schemaString.split(" ").map(fieldName =>
        StructField(fieldName, StringType, true)))

    val postsDf = sqlContext.createDataFrame(postsRDD, schema)

    postsDf.show()

  }
}
