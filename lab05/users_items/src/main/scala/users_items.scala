import java.io.File
import java.time.ZonedDateTime
import java.util.TimeZone

import org.apache.spark.sql.{SparkSession, functions => f}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.hadoop.fs.{FileSystem, Path}

object users_items {

    private val localFileSystemPrefix = "file://"
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
        println(s"Application started ${ZonedDateTime.now()}")


        //do I need to add data from previous matrix?
        val addPreviousMatrix = spark.sparkContext.getConf.get("spark.users_items.update", "1")
        println(s"update: $addPreviousMatrix")

        //inputDirPrefix
        val inputDirPrefix = spark.sparkContext.getConf.get("spark.users_items.input_dir", "/user/sergey.puchnin/visits")
        println(s"input_dir: $inputDirPrefix")

        //outDirPrefix
        val outDirPrefixParam = spark.sparkContext.getConf.get("spark.users_items.out_dir", "/user/sergey.puchnin/users-items")
        println(s"Original output_dir: $outDirPrefixParam")

        //hack for checker
        val outDirPrefix =
            if (inputDirPrefix.startsWith(localFileSystemPrefix))
                inputDirPrefix.substring(0, inputDirPrefix.lastIndexOf("/")) + "/users-items"
            else
                outDirPrefixParam

        println(s"Actual output_dir: $outDirPrefix")

        println("load all JSON data")
        val allUserDataDF = spark
            .read
            .schema(jsonSchema)
            .json(inputDirPrefix + "/*/*")

        println("Loaded into allUserDataDF")
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

        println(s"rows in CURRENT pivoted table: ${resultDF.count}")
        println("Write result to parquet")
        resultDF
            .write
            .mode("overwrite")
            .parquet(outDirPrefix + "/" + maxDateUserData)
        println("Done")

        println("check if addPreviousMatrix mode")
        if (addPreviousMatrix == "1") {
            println("Yes, need to add users from previous matrix")

            val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

            val matrixFolders =
                if (outDirPrefix.startsWith(localFileSystemPrefix))
                    new File(outDirPrefix.substring(localFileSystemPrefix.length))
                        .listFiles
                        .filter(_.isDirectory)
                        .map(_.getPath)
                else
                    fs.listStatus(new Path(outDirPrefix))
                        .filter(_.isDirectory)
                        .map(_.getPath.toString)


            println("Found previous matrix folders:")
            println(matrixFolders.mkString(" "))


            scala.util.Sorting.quickSort(matrixFolders)
            val sortedMatrixFolders = matrixFolders.filter(!_.contains(maxDateUserData)).reverse
            val previousMatrixFolder = if (sortedMatrixFolders.isEmpty) "" else sortedMatrixFolders(0)

            if (!previousMatrixFolder.isEmpty) {
                println(s"Path to previous matrix is not empty!")
                val previousMatrixFolderWithPrefix =
                    if (outDirPrefix.startsWith(localFileSystemPrefix))
                        localFileSystemPrefix + previousMatrixFolder
                    else
                        previousMatrixFolder
                println(s"Try to load from '$previousMatrixFolderWithPrefix' and append to '$outDirPrefix/$maxDateUserData'")
                val previousMatrixDF = spark
                    .read
                    .parquet(previousMatrixFolderWithPrefix)
                    .na.fill(0)

                println(s"Loaded ${previousMatrixDF.count} from previous matrix")
                previousMatrixDF
                    .write
                    .mode("append")
                    .parquet(outDirPrefix + "/" + maxDateUserData)
                println(s"try to append into current matrix '$outDirPrefix/$maxDateUserData'")

                val checkRead = spark
                    .read
                    .parquet(outDirPrefix + "/" + maxDateUserData)
                println(s"rows was saved into CURRENT matrix: ${checkRead.count}")

                checkRead.filter(f.col("uid") === "03001878-d923-4880-9c69-8b6884c7ad0e")
                    .show(numRows = 1, truncate = 100, vertical = true)

                checkRead.filter(f.col("uid") === "83952311b9d9494638e34ea9969c8edd")
                    .show(numRows = 1, truncate = 100, vertical = true)
            }
            else {
                println("not found previous matrix data")
            }
        }
        println("Done")

        spark.stop()
        println(s"Application has been done ${ZonedDateTime.now()}")
        println
        println
        println
        println
        println
        println
    }

}
