package com.npl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{LongType, StructField, StructType, StringType}
import org.scalatest.{FlatSpec, Matchers}

class MyDataSourceSpec extends FlatSpec with Matchers {

    private val spark = SparkSession.builder().master(master = "local[1]").getOrCreate()
    private val schema = StructType(
        StructField("id", LongType) ::
            StructField("foo", StringType) ::
            Nil
    )
    private val df = spark.readStream
        .format(source = "org.apache.spark.sql.npl.MyDataSourceProvider")
        .schema(schema)
        .option("step", 10)
        .load()
        .groupBy("foo")
        .count

    df.writeStream
        .outputMode("complete")
        .options(Map("la"-> "foo"))
        .option("checkpointLocation", "target/ch3")
        .format(source = "org.apache.spark.sql.npl.MyDataSinkProvider")
        .trigger(Trigger.ProcessingTime(5000))
        .start()
        .awaitTermination(20000)
}
