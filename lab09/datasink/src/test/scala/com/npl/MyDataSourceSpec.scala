package com.npl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{LongType, StructField, StructType, StringType}
import org.scalatest.{FlatSpec, Matchers}

class MyDataSourceSpec extends FlatSpec with Matchers {

    val spark = SparkSession.builder().master(master = "local[1]").getOrCreate()
    val schema = StructType(
        StructField("id", LongType)::
        StructField("foo", StringType)::
        Nil
    )
    val df = spark.readStream
        .format(source = "org.apache.spark.sql.npl.MyDataSource")
        .schema(schema)
        .load()

    df.writeStream
        //.option("checkpointLocation", "target/ch2")
        .format("console")
        .trigger(Trigger.ProcessingTime(5000))
        .start()
        .awaitTermination(20000)
}
