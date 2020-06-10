import java.time.ZonedDateTime
import java.util.TimeZone

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}
import org.apache.spark.sql.{SparkSession, functions => f}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


object agg {

    private val batchTimeSeconds = 5
    //private val checkpointDirectory = "hdfs:///streamingCheckpoints/"
    private val checkpointDirectory = "streamingCheckpoints"
    private val kafkaBrokers = "10.0.1.13:6667"
    private val topicNameIn = "sergey_puchnin"
    private val topicNameOut = "sergey_puchnin_lab04b_out"

    private val eventScheme = StructType(
        Array(
            StructField("event_type", StringType),
            StructField("category", StringType),
            StructField("item_id", StringType),
            StructField("item_price", LongType),
            StructField("uid", StringType),
            StructField("timestamp", LongType)
        )
    )


    def main(args: Array[String]) {

        val spark = SparkSession
            .builder()
            .appName("sergey.puchnin lab04a")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

        println
        println
        println
        println

        val kafkaStreamDF = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBrokers)
            .option("subscribe", topicNameIn)
            .load()


        val df1 = kafkaStreamDF
            .select(f.from_json(f.col("value").cast("string"), eventScheme).as("json"))
            .select("json.*")
            .withColumn("eventTime", f.from_unixtime(f.col("timestamp") / 1000))
            .groupBy(f.window(f.col("eventTime"), "1 hour").as("ts"))
            .agg(
                f.sum(f.when(f.col("uid").isNotNull, 1)).as("visitors"),
                f.sum(f.when(f.col("event_type") === "buy", f.col("item_price"))).as("revenue"),
                f.sum(f.when(f.col("event_type") === "buy", 1)).as("purchases")
            )
            .select(
                f.to_timestamp(f.col("ts").getField("start")).as("start_ts"),
                f.to_timestamp(f.col("ts").getField("end")).as("end_ts"),
                f.col("revenue"),
                f.col("visitors"),
                f.col("purchases"),
                (f.col("revenue") / f.col("purchases")).as("aov")
            )


        /*
        val consoleOutput = df1
            .writeStream
            .format("console")
            .outputMode("update")
            .trigger(Trigger.ProcessingTime(s"$batchTimeSeconds seconds"))
            .start()
            .awaitTermination()
        */


        val kafkaOutput = df1.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBrokers)
            .option("topic", topicNameOut)
            .trigger(Trigger.ProcessingTime(s"$batchTimeSeconds seconds"))
            .option("checkpointLocation", checkpointDirectory)
            .start()
            .awaitTermination()

    }
}
