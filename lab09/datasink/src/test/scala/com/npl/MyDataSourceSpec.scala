package com.npl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.{FlatSpec, Matchers}

class MyDataSourceSpec extends FlatSpec with Matchers {

    val spark = SparkSession.builder().master(master = "local[1]").getOrCreate()
    val df = spark.readStream.format(source = "org.apache.spark.sql.npl.MyDataSource").load()

    df.writeStream.format("console").trigger(Trigger.Once()).start().awaitTermination()
}
