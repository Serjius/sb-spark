import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, functions => f}

object test extends SparkSupport {


    def main(args: Array[String]): Unit = {
        val config = getConf
        val model = loadModel(config)
        val rawDF = readKafka(config)
        val cleanedDF = cleanup(rawDF)
        val userDF = transform(cleanedDF)
        val resDF = predict(model, userDF)
        saveResult(config, resDF)

        spark.stop()
    }

    case class lab07TestConfig(
                                  modelPath: String,
                                  kafkaBroker: String,
                                  inputTopic: String,
                                  outputTopic: String,
                                  checkpointPath: String
                              )

    private val eventScheme = StructType(
        Array(
            StructField("uid", StringType),
            StructField("visits", ArrayType(StructType(Array(
                StructField("url", StringType),
                StructField("timestamp", LongType)
            ))))
        )
    )

    def getConf: lab07TestConfig = {
        import org.apache.hadoop.fs.{FileSystem, Path}

        val modelPath = spark.sparkContext.getConf.get("spark.test.model.input_path")
        val kafkaBroker = spark.sparkContext.getConf.get("spark.test.kafka.broker")
        val inputTopic = spark.sparkContext.getConf.get("spark.test.kafka.input_topic")
        val outputTopic = spark.sparkContext.getConf.get("spark.test.kafka.output_topic")
        val checkpointPath = spark.sparkContext.getConf.get("spark.test.kafka.checkPointsPath")


        FileSystem.get(new org.apache.hadoop.conf.Configuration).delete(new Path(checkpointPath), true)

        val config = lab07TestConfig(
            modelPath,
            kafkaBroker,
            inputTopic,
            outputTopic,
            checkpointPath
        )
        println("Config:")
        println(config)
        config
    }

    def loadModel(config: test.lab07TestConfig): PipelineModel = {
        PipelineModel.load(config.modelPath)
    }

    def readKafka(conf: lab07TestConfig): DataFrame = {
        val df: DataFrame = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", conf.kafkaBroker)
            .option("startingOffsets", "latest")
            .option("subscribe", conf.inputTopic)
            .load()
        df
    }

    def cleanup(userDF: DataFrame): DataFrame = {
        val df = userDF
            .select(f.from_json(f.col("value").cast("string"), eventScheme).as("json"))
            .select("json.*")
            .withColumn("visitsList", f.explode(f.col("visits")))
            .withColumn("web_url", f.col("visitsList.url"))
            .withColumn("domain", f.lower(f.callUDF("parse_url", f.col("web_url"), f.lit("HOST"))))
            .withColumn("domain", f.regexp_replace(f.col("domain"), "^www.", ""))
            //.filter(f.col("domain").isNotNull)

        df.select("uid", "domain")
    }

    def transform(cleanedDF: DataFrame): DataFrame = {
        val df = cleanedDF
            .select("uid", "domain")
            .groupBy("uid")
            .agg(f.collect_list("domain").alias("domains"))

        df.select("uid", "domains")
    }

    def predict(model: PipelineModel, df: DataFrame): DataFrame = {
        val predictDF = model.transform(df)
        predictDF
            .select(f.col("uid"), f.col("predictedLabel").as("gender_age"))
    }


    def saveResult(config: lab07TestConfig, df: DataFrame): Unit = {
        df
            .toJSON
            .writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", config.kafkaBroker)
            .option("topic", config.outputTopic)
            .outputMode("update")
            .trigger(Trigger.ProcessingTime("10 seconds"))
            .option("checkpointLocation", config.checkpointPath)
            .start
            .awaitTermination
    }


}
