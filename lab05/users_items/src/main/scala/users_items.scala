import java.io.File
import java.time.ZonedDateTime
import java.util.TimeZone

import org.apache.spark.sql.{SparkSession, functions => f}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object users_items {

    private val jsonSchema = StructType(
        Array(
            StructField("event_type", StringType),
            StructField("category", StringType),
            StructField("item_id", StringType),
            StructField("item_price", LongType),
            StructField("uid", StringType),
            StructField("timestamp", LongType),
            StructField("date", StringType)
        )
    )

    def main(args: Array[String]) {

        val spark = SparkSession
            .builder()
            .appName("sergey.puchnin lab05")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"))


        println
        println
        println
        println
        println(s"Application started ${println(ZonedDateTime.now())}")


        //do I need to add data from previous matrix?
        val addPreviousMatrix = spark.sparkContext.getConf.get("spark.users_items.update", "1")
        println(s"update: $addPreviousMatrix")

        //inputDirPrefix
        val inputDirPrefix = spark.sparkContext.getConf.get("spark.users_items.input_dir", "/user/sergey.puchnin/visits")
        println(s"input_dir: $inputDirPrefix")

        //outDirPrefix
        val outDirPrefix = spark.sparkContext.getConf.get("spark.users_items.out_dir", "/user/sergey.puchnin/users-items")
        println(s"out_dir: $outDirPrefix")


        println("load all JSON data")
        val allUserDataDF = spark
            .read
            .schema(jsonSchema)
            .json(inputDirPrefix + "/*/*")

        println("allUserDataDF")
        allUserDataDF.printSchema()
        allUserDataDF.show(false)
        println(s"all user count ${allUserDataDF.count}")

        val maxDateUserData = allUserDataDF.agg(f.max("date")).head().getString(0)
        println(s"Max date $maxDateUserData  ${maxDateUserData.getClass}")


        println("generate item name")
        val itemsDF = allUserDataDF
            .select(f.col("uid"), f.col("item_id"), f.col("event_type"), f.lit(1).alias("count"))
            .filter(f.col("uid").isNotNull)
            .withColumn("item_name", f.regexp_replace(f.col("item_id"), "-", "_"))
            .withColumn("item_name", f.regexp_replace(f.col("item_name"), " ", "_"))
            .withColumn("item_name", f.lower(f.col("item_name")))
            .withColumn("item_name", f.concat(f.col("event_type"), f.lit("_"), f.col("item_name")))
            .cache

        println("itemsDF")
        itemsDF.printSchema()
        itemsDF.show(false)
        println(s"all user count ${itemsDF.count}")


        println("distinct column list")
        val itemList = itemsDF.select("item_name").distinct().collect().map(_ (0)).toList
        println(s"distinct items categories for NOT NULL users ${itemList.size}")

        println("pivot table")
        val resultDF = itemsDF
            .groupBy("uid")
            .pivot("item_name", itemList)
            .sum("count")
            .na.fill(0)
            .cache()

        println(s"row in pivoted table: ${resultDF.count}")
        println("Write result to parquet")
        resultDF
            .write
            .mode("overwrite")
            .parquet(outDirPrefix + "/" + maxDateUserData)
        println("Done")

        println("check if addPreviousMatrix mode and local file systems")
        if (addPreviousMatrix == "1" && outDirPrefix.startsWith("file:///")) {
            println("Yes, need to add users from previous matrix")
            val folders = new File(outDirPrefix).listFiles.filter(_.isDirectory)
            val folderNameArray = for (x <- folders) yield x.getName
            println(s"Matrix folders list: ${folderNameArray.mkString(" ")}")
            scala.util.Sorting.quickSort(folderNameArray)
            //exclude current matrix
            val previousMatrixFolder = folderNameArray.filter(!_.contains(maxDateUserData)).reverse(0)
            println(s"Previous matrix folder $previousMatrixFolder")

            if (!previousMatrixFolder.isEmpty) {
                val previousMatrixDF = spark
                    .read
                    .parquet(outDirPrefix + "/" + previousMatrixFolder)

                previousMatrixDF
                    .write
                    .mode("overwrite")
                    .parquet(outDirPrefix + "/" + maxDateUserData)
            }
        }
        println("Done")

        spark.stop()
        println(s"Application has been done ${println(ZonedDateTime.now())}")
    }

}
