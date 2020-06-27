import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{CountVectorizer, IndexToString, StringIndexer}
import org.apache.spark.sql.{DataFrame, functions => f}

case class lab07TrainConfig(
                               inputFileName: String,
                               testUserId: String,
                               modelPath: String,
                               modelSeed: Int
                           )

object train extends SparkSupport {


    def main(args: Array[String]): Unit = {
        val config = getConf
        val logsDF = readWebLogs(config)
        val cleanedDF = cleanData(config, logsDF)
        val featureDF = transform(config, cleanedDF)
        val resDF = trainModel(config, featureDF)
        saveResult(config, resDF)

        spark.stop()
    }

    def getConf: lab07TrainConfig = {
        val inputDir = spark.sparkContext.getConf.get("spark.train.input_file")
        val testUser = spark.sparkContext.getConf.get("spark.train.test_user.id")
        val modelPath = spark.sparkContext.getConf.get("spark.train.model.output_path")
        val modelSeed = spark.sparkContext.getConf.get("spark.train.model.seed").toInt

        val config = lab07TrainConfig(
            inputDir,
            testUser,
            modelPath,
            modelSeed
        )
        println("Config:")
        println(config)
        config
    }

    def readWebLogs(config: lab07TrainConfig): DataFrame = {

        val webLogsDF = spark
            .read
            .json(config.inputFileName)
            .toDF()
            .filter(f.col("uid").isNotNull)

        println(s"Data has been loaded. Rows: ${webLogsDF.count}")
        println("Show test user data after load")
        webLogsDF.filter(f.col("uid") === config.testUserId).show(5, 120, vertical = true)

        webLogsDF

    }

    def cleanData(conf: lab07TrainConfig, df: DataFrame): DataFrame = {
        val cleanDF = df
            .select("uid", "gender_age", "visits")
            .withColumn("visitsList", f.explode(f.col("visits")))
            .withColumn("web_url", f.col("visitsList.url"))
            .withColumn("domain", f.lower(f.callUDF("parse_url", f.col("web_url"), f.lit("HOST"))))
            .withColumn("domain", f.regexp_replace(f.col("domain"), "^www.", ""))
            .filter(f.col("domain").isNotNull)

        val tstFD = cleanDF.filter(f.col("uid") === conf.testUserId)
        println("Show test user data after cleaned")
        tstFD.show(10, 120, vertical = true)

        println("Cleaned data schema")
        println(cleanDF.printSchema)

        cleanDF
    }

    def transform(conf: lab07TrainConfig, df: DataFrame): DataFrame = {
        val onlyFeatureDF = df
            .select("uid", "gender_age", "domain")
            .groupBy("uid", "gender_age")
            .agg(f.collect_list("domain").alias("domains"))

        val tstFD = onlyFeatureDF.filter(f.col("uid") === conf.testUserId)
        println("Show test user data after cleaned")
        tstFD.show(10, 120, vertical = true)

        onlyFeatureDF
    }

    def trainModel(conf: lab07TrainConfig, df: DataFrame): PipelineModel = {

        val cv = new CountVectorizer().setInputCol("domains").setOutputCol("features")

        val str2idx = new StringIndexer()
            .setInputCol("gender_age")
            .setOutputCol("label")
            .fit(df)

        val lr = new LogisticRegression()
            .setMaxIter(100)
            .setRegParam(0.001)
            .setLabelCol("label")
            .setFeaturesCol("features")
            .setPredictionCol("prediction")

        val idx2str = new IndexToString()
            .setInputCol("prediction")
            .setOutputCol("predictedLabel")
            .setLabels(str2idx.labels)

        val pipeline = new Pipeline().setStages(Array(cv, str2idx, lr, idx2str))

        val splitSeed = conf.modelSeed
        val Array(train, test) = df.randomSplit(Array(0.7, 0.3), splitSeed)

        val model = pipeline.fit(train)

        val predictions = model.transform(test)
        predictions
            .select("gender_age", "label", "predictedLabel", "prediction")
            .show(numRows = 20, truncate = 120, vertical = false)

        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("label")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")
            .setLabelCol("label")

        val accuracy = evaluator.evaluate(predictions)

        println("Accuracy = " + accuracy)
        println(s"Test Error = ${1 - accuracy}")

        model
    }

    def saveResult(config: lab07TrainConfig, model: PipelineModel): Unit = {
        model.write.overwrite().save(config.modelPath)
    }

}
