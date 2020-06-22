import org.apache.spark.sql.{DataFrame, functions => f}
import java.time.ZonedDateTime
import java.util.TimeZone

import org.apache.spark.ml.feature.{CountVectorizer}


object features extends SparkSupport {
    private val WebLogs = "hdfs:///labs/laba03/weblogs.json"
    private val TestUserId = "d50192e5-c44e-4ae8-ae7a-7cfe67c8b777"
    private val TopDomain = 1000
    private val ItemMatrixFile = "users-items/20200429"
    private val OutPutFolder = "features"


    def main(args: Array[String]): Unit = {


        TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
        println
        println
        println
        println
        println("Application started")
        println(ZonedDateTime.now())

        val webLogsDF = loadData(WebLogs)
        println(s"Data has been loaded. Rows: ${webLogsDF.count}")

        val cleanedDF = cleanData(webLogsDF)
        cleanedDF.cache

        println(s"Data has been cleaned. Rows: ${cleanedDF.count}")
        println(s"Describe cleanedDF")
        describeDF(cleanedDF)

        //showTestUser(cleanedDF, TestUserId)

        //check broken parsing
        //cleanedDF.filter(f.col("domain") === "http").show(numRows = 10, truncate = 120, vertical = true)
        //cleanedDF.filter(f.col("domain") === "https").show(numRows = 10, truncate = 120, vertical = true)
        //cleanedDF.filter(f.col("domain") === ".kasparov.ru").show(numRows = 10, truncate = 120, vertical = true)


        //Get week day metrics
        val pivotedDaysDF = cleanedDF
            .select("uid", "web_week_day")
            .groupBy("uid")
            .pivot("web_week_day")
            .count
        println("Describe pivotedDaysDF")
        describeDF(pivotedDaysDF)
        println()
        println()

        //Get hours metrics
        val pivotedHoursDF = cleanedDF
            .select("uid", "web_hour")
            .groupBy("uid")
            .pivot("web_hour")
            .count
        println("Describe pivotedHoursDF")
        describeDF(pivotedHoursDF)
        println()
        println()

        //get fraction hour metrics
        val hoursSumDF = cleanedDF
            .select(f.col("uid"), f.col("work_hours"), f.col("evening_hours"), f.lit(1).alias("count"))
            .groupBy("uid")
            .agg(f.sum("work_hours").alias("sum_work_hours")
                , f.sum("evening_hours").alias("sum_evening_hours")
                , f.sum("count").alias("total_count")
            )

        val hoursFractionDF = hoursSumDF
            .select("uid", "sum_work_hours", "sum_evening_hours", "total_count")
            .withColumn("web_fraction_work_hours", f.col("sum_work_hours") / f.col("total_count"))
            .withColumn("web_fraction_evening_hours", f.col("sum_evening_hours") / f.col("total_count"))


        //Get features
        val top1000 = getTopDomain(cleanedDF, TopDomain)
        val onlyTop1000VisitsDF = top1000.join(cleanedDF, Seq("domain"), "inner")
        val onlyFeatureDF = onlyTop1000VisitsDF
            .select("uid", "domain")
            .groupBy("uid")
            .agg(f.collect_list("domain").alias("domain"))
            .cache

        /*

        val tokenizer = new Tokenizer().setInputCol("domain").setOutputCol("words")
        val wordsDF = tokenizer.transform(onlyFeatureDF)
        wordsDF.cache()
*/

        val countVectorizer = new CountVectorizer().setInputCol("domain").setOutputCol("domain_features")
        val countVectorModel = countVectorizer.fit(onlyFeatureDF)
        val featureDF = countVectorModel.transform(onlyFeatureDF)


        //Gather total DF
        val totalDF = featureDF
            .join(pivotedDaysDF, Seq("uid"), "inner")
            .join(pivotedHoursDF, Seq("uid"), "inner")
            .join(hoursFractionDF, Seq("uid"), "inner")
            .drop("domain")
            .drop("sum_work_hours")
            .drop("sum_evening_hours")
            .drop("total_count")
            .na.fill(0)
        println("Join to final log DF")
        println(s"Count in ${totalDF.count}")
        totalDF.printSchema

        val itemsDF = spark.read.parquet(ItemMatrixFile)
        println(s"itemsDF rows ${itemsDF.count}")
        itemsDF.printSchema

        val finalDF = totalDF.join(itemsDF, Seq("uid"), "inner")
        finalDF
            .write
            .mode("overwrite")
            .parquet(s"$OutPutFolder")
        println("Save done")

        println
        println
        println
        println
        println
        println(s"finalDF rows ${finalDF.count}")
        finalDF.printSchema


        println()
        println()
        println()
        println()
        println("Check user#1")
        println()
        println()

        println("User#1 Data from cleanedDF")
        cleanedDF
            .filter(f.col("uid") === TestUserId)
            .show(numRows = 10, truncate = 120, vertical = true)

        println("User#1 Data from pivotedDaysDF")
        pivotedDaysDF
            .filter(f.col("uid") === TestUserId)
            .show(truncate = false)

        println("User#1 Data from pivotedHoursDF")
        pivotedHoursDF
            .filter(f.col("uid") === TestUserId)
            .show(numRows = 10, truncate = 120, vertical = true)

        println("User#1 Data from hoursFractionDF")
        hoursFractionDF
            .filter(f.col("uid") === TestUserId)
            .show(numRows = 10, truncate = 120, vertical = false)

        println("User#1 Data from onlyFeatureDF")
        onlyFeatureDF
            .filter(f.col("uid") === TestUserId)
            .show(numRows = 10, truncate = 120, vertical = false)

        println("User#1 Data from featureDF")
        featureDF
            .filter(f.col("uid") === TestUserId)
            .show(numRows = 10, truncate = 120, vertical = false)

        println("User#1 Data from TotalDF")
        totalDF
            .filter(f.col("uid") === TestUserId)
            .show(numRows = 10, truncate = 120, vertical = true)
        //
        //        println("User#1 Data from wordsDF")
        //        wordsDF
        //            .filter(f.col("uid") === TestUserId)
        //            .show(numRows = 10, truncate = 120, vertical = false)



        spark.stop()
        println("Application has been done")
    }


    private def loadData(fileName: String) = {
        spark
            .read
            .json(fileName)
            .toDF()
    }

    private def cleanData(df: DataFrame) = {
        df
            .select(f.col("uid"), f.explode(f.col("visits")).alias("visitsList"))
            .withColumn("web_url", f.col("visitsList.url"))
            .withColumn("domain", f.lower(f.callUDF("parse_url", f.col("web_url"), f.lit("HOST"))))
            .withColumn("domain", f.regexp_replace(f.col("domain"), "^www.", ""))
            .withColumn("date", f.from_unixtime(f.col("visitsList.timestamp") / 1000))
            .withColumn("date", f.date_format(f.col("date"), "yyyy-MM-dd HH:mm:ss.SSSS"))
            .withColumn("web_week_day", f.concat(f.lit("web_day_"), f.lower(f.date_format(f.col("date"), "E"))))
            .withColumn("web_hour", f.concat(f.lit("web_hour_"), f.hour(f.col("date"))))
            .withColumn("work_hours",
                f.when(f.hour(f.col("date")) >= 9 and f.hour(f.col("date")) < 18, 1)
                    .otherwise(0)
            )
            .withColumn("evening_hours",
                f.when(f.hour(f.col("date")) >= 18 and f.hour(f.col("date")) < 24, 1)
                    .otherwise(0)
            )
            .filter(f.col("domain").isNotNull)
            .filter(f.col("uid").isNotNull)
    }

    private def describeDF(df: DataFrame, showNumber: Int = 1, showInLine: Boolean = true): Unit = {
        df.printSchema()
        df.show(numRows = showNumber, truncate = 120, vertical = showInLine)
    }

    private def getTopDomain(df: DataFrame, topCount: Int) = {
        df
            .groupBy("domain")
            .count()
            .orderBy(f.col("count").desc)
            .limit(topCount)
            .orderBy(f.col("domain"))
    }

}
