import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, functions => f}

import scala.io.Source


object dashboard extends SparkSupport {
    def main(args: Array[String]): Unit = {
        val config = getConf
        val model = loadModel(config)
        val rawDF = readWebLogs(config)
        val cleanedDF = cleanup(rawDF)
        val userDF = transform(cleanedDF)
        val resDF = predict(model, userDF)
        saveResult(config, resDF)

        spark.stop()
    }

    case class lab08Config(
                              modelPath: String,
                              dataPath: String,
                              elasticNodes: String,
                              elasticUser: String,
                              elasticPassword: String,
                              elasticIndex: String
                          )

    private def loadPassword(): String = {

        try {
            val in = "secret.txt"
            val src = Source.fromFile(in)
            val strArr = src.getLines.toArray
            src.close()
            strArr(0)
        }
        catch {
            case e: Throwable =>
                println(e.getMessage)
                println(e.getStackTrace.mkString("\n"))
                System.exit(-1)
                ""
        }
    }

    def getConf: lab08Config = {
        val modelPath = spark.sparkContext.getConf.get("spark.dashboard.model.path")
        val dataPath = spark.sparkContext.getConf.get("spark.dashboard.data.path")
        val elasticNodes = spark.sparkContext.getConf.get("spark.dashboard.es.nodes")
        val elasticUser = spark.sparkContext.getConf.get("spark.dashboard.es.http.auth.user")
        val elasticIndex = spark.sparkContext.getConf.get("spark.dashboard.es.index.name")
        val elasticPassword = loadPassword()

        val config = lab08Config(
            modelPath,
            dataPath,
            elasticNodes,
            elasticUser,
            elasticPassword,
            elasticIndex
        )
        println("Config:")
        println(config)
        config
    }

    def loadModel(config: lab08Config): PipelineModel = {
        PipelineModel.load(config.modelPath)
    }

    def readWebLogs(conf: lab08Config): DataFrame = {
        val df: DataFrame = spark
            .read
            .json(conf.dataPath)
            .toDF()
        println(s"Rows in web logs: ${df.count()}")
        println(df.printSchema())
        df.show(5, 100)

        df
    }

    def cleanup(userDF: DataFrame): DataFrame = {
        val df = userDF
            .withColumn("visitsList", f.explode(f.col("visits")))
            .withColumn("web_url", f.col("visitsList.url"))
            .withColumn("domain", f.lower(f.callUDF("parse_url", f.col("web_url"), f.lit("HOST"))))
            .withColumn("domain", f.regexp_replace(f.col("domain"), "^www.", ""))
            .select("date", "uid", "domain")

        df.show(10, 120)
        df
    }

    def transform(cleanedDF: DataFrame): DataFrame = {
        val df = cleanedDF
            .select("uid", "date", "domain")
            .groupBy("date", "uid")
            .agg(f.collect_list("domain").alias("domains"))
            .select("date", "uid", "domains")

        df.show(10, 120)
        df

    }

    def predict(model: PipelineModel, df: DataFrame): DataFrame = {
        val predictDF = model.transform(df)
            .select(f.col("uid")
                , f.col("predictedLabel").as("gender_age")
                , f.col("date")
            )

        predictDF.show(10, 120)
        predictDF
    }

    def saveResult(config: lab08Config, df: DataFrame): Unit = {
        val esOptions =
            Map(
                "es.nodes" -> config.elasticNodes,
                "es.batch.write.refresh" -> "false",
                "es.net.http.auth.user" -> config.elasticUser,
                "es.net.http.auth.pass" -> config.elasticPassword
            )

        df.toJSON
            .write
            .mode("append")
            .format("org.elasticsearch.spark.sql")
            .options(esOptions)
            .save(s"${config.elasticIndex}/_doc")
    }
}
