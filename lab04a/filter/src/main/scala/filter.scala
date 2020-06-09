import java.time.ZonedDateTime
import java.util.TimeZone

import org.apache.spark.sql.{SaveMode, SparkSession, functions => f}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.hadoop.fs.{FileSystem, Path}


object filter {

    private val kafkaBrokers = "10.0.1.13:6667"

    def main(args: Array[String]) {

        val spark = SparkSession
            .builder()
            .appName("sergey.puchnin lab04a")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"))


        println
        println
        println
        println
        println("Application started")

        println(ZonedDateTime.now())

        val topicName = spark.sparkContext.getConf.get("spark.filter.topic_name", "lab04_input_data")
        println(topicName)

        val topicOffsetParam = spark.sparkContext.getConf.get("spark.filter.offset", "earliest")
        val topicOffsets = if (topicOffsetParam == "earliest") "earliest" else s"""{"$topicName":{"0":$topicOffsetParam}}"""

        println(topicOffsets)
        var output_dir_prefix = spark.sparkContext.getConf.get("spark.filter.output_dir_prefix", "hdfs:///user/sergey.puchnin/visits")

        if (!(output_dir_prefix.startsWith("hdfs:///") || output_dir_prefix.startsWith("file:///"))) {
            output_dir_prefix = "hdfs:///" + output_dir_prefix
        }

        println(output_dir_prefix)


        val schema = StructType(
            Array(
                StructField("event_type", StringType),
                StructField("category", StringType),
                StructField("item_id", StringType),
                StructField("item_price", LongType),
                StructField("uid", StringType),
                StructField("timestamp", LongType)
            )
        )

        val kafkaDF = spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBrokers)
            .option("subscribe", topicName)
            .option("startingOffsets", topicOffsets)
            .load
            .cache
        kafkaDF.printSchema
        kafkaDF.show
        println(kafkaDF.count)

        val eventDF = kafkaDF
            .select(f.from_json(f.col("value").cast("string"), schema).as("jsonData"))
            .select("jsonData.*")
            .withColumn("date", f.from_unixtime(f.col("timestamp") / 1000))
            .withColumn("date", f.date_format(f.col("date"), "yyyyMMdd"))
            .orderBy(f.col("date").desc)
            .cache

        eventDF.printSchema()
        eventDF.show()
        println(eventDF.count())


        //Work with VIEW
        val viewSuffix = "view"
        println("Work with VIEW")

        println("Clean up")
        //Remove previous tries
        val viewFS = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val viewPath = new Path(output_dir_prefix + "/" + viewSuffix)
        if (viewFS.exists(viewPath))
            viewFS.delete(viewPath, true)


        //filter only view and cache it
        val onlyViewDF = eventDF
            .filter(f.col("event_type") === viewSuffix).cache
        val dateViewList = onlyViewDF.select(f.col("date")).distinct().collect()
        println("in view section " + onlyViewDF.count())


        println("Start save " + ZonedDateTime.now())
        for (date <- dateViewList) {
            val jsonViewDF = onlyViewDF
                .filter(f.col("date") === date(0))
                .write
                .mode(SaveMode.Append)
                .format("json")
                .save(output_dir_prefix + s"/$viewSuffix/${date(0)}")
        }
        println("Save done " + ZonedDateTime.now())

        onlyViewDF.unpersist()




        //Work with BUY
        val buySuffix = "buy"

        println("Work with BUY")
        println("Clean up")
        //Remove previous tries
        val BuyFS = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val buyPath = new Path(output_dir_prefix + "/" + buySuffix)
        if (BuyFS.exists(buyPath))
            BuyFS.delete(buyPath, true)


        //filter only view and cache it
        val onlyBuyDF = eventDF
            .filter(f.col("event_type") === buySuffix).cache
        val dateBuyList = onlyViewDF.select(f.col("date")).distinct().collect()
        println("in buy section " + onlyBuyDF.count())


        println("Start save " + ZonedDateTime.now())
        for (date <- dateBuyList) {
            val jsonBuyDF = onlyBuyDF
                .filter(f.col("date") === date(0))
                .write
                .mode(SaveMode.Append)
                .format("json")
                .save(output_dir_prefix + s"/$buySuffix/${date(0)}")
        }
        println("Save done " + ZonedDateTime.now())

        onlyBuyDF.unpersist()


        spark.stop()
        println("Application has been done")
    }
}