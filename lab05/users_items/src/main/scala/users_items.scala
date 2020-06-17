import java.io.File
import java.time.ZonedDateTime
import java.util.TimeZone

import org.apache.spark.sql.{DataFrame, SparkSession, functions => f}
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


        //Load data
        println("load all JSON data")
        val allUserDataDF = spark
            .read
            .schema(jsonSchema)
            .json(inputDirPrefix + "/*/*")

        println("Loaded into allUserDataDF")
        allUserDataDF.printSchema()
        allUserDataDF.show(false)
        println(s"all user count ${allUserDataDF.count}")

        //Get Max Data
        val maxDateUserData = allUserDataDF.agg(f.max("date")).head().getString(0)
        println(s"Max date $maxDateUserData  ${maxDateUserData.getClass}")

        //Clear Data
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

        val resultDF = pivotItems(itemsDF)
        println(s"rows in CURRENT pivoted table: ${resultDF.count}")

        println("check if addPreviousMatrix mode enabled")
        val previousMatrixDF = loadPrevMatrixOrEmpty(spark = spark, loadPrevMatrixFlag = addPreviousMatrix, currentMatrixDir = maxDateUserData, outDir = outDirPrefix)
        println(s"from previous matrix loaded: ${previousMatrixDF.count}")

        println("Union 2 matrix")
        val unionDF = union2Matrix(resultDF, previousMatrixDF)
        println(s"Rows in union matrix: ${unionDF.count}")

        println(s"Save union result to parquet to $outDirPrefix/$maxDateUserData")
        unionDF
            .write
            .mode("overwrite")
            .parquet(s"$outDirPrefix/$maxDateUserData")
        println("Save done")


        checkResult(spark = spark, currentMatrixDir = maxDateUserData, outDir = outDirPrefix)

        spark.stop()
        println(s"Application has been done ${ZonedDateTime.now()}")
        println
        println
        println
        println
        println
        println
    }

    private def union2Matrix(df1: DataFrame, df2: DataFrame): DataFrame = {
        val uniCols = df1.columns.union(df2.columns).toSet
        val df1Cols = df1.columns.toSet
        val df2Cols = df2.columns.toSet

        def addMissedFields(myCols: Set[String], unionCols: Set[String]) = {
            unionCols.toList.map {
                case x if myCols.contains(x) => f.col(x)
                case x => f.lit(null).as(x)
            }
        }

        df1.select(addMissedFields(df1Cols, uniCols): _*).unionByName(df2.select(addMissedFields(df2Cols, uniCols): _*))


    }

    private def pivotItems(df: DataFrame) = {

        println("Form distinct column list")
        val itemList = df.select("item_name").distinct().collect().map(_ (0)).toList
        println(s"distinct items categories for NOT NULL users ${itemList.size}")

        println("pivot table")
        df
            .groupBy("uid")
            .pivot("item_name", itemList)
            .sum("count")
            .na.fill(0)
            .cache()
    }

    def checkResult(spark: SparkSession, outDir: String, currentMatrixDir: String): Unit = {
        println("Load 2 users from result for check ")
        val checkRead = spark
            .read
            .parquet(s"$outDir/$currentMatrixDir")
        println(s"rows was saved into CURRENT matrix: ${checkRead.count}")

        checkRead.filter(f.col("uid") === "03001878-d923-4880-9c69-8b6884c7ad0e")
            .show(numRows = 1, truncate = 100, vertical = true)

        checkRead.filter(f.col("uid") === "83952311b9d9494638e34ea9969c8edd")
            .show(numRows = 1, truncate = 100, vertical = true)
    }

    private def loadPrevMatrixOrEmpty(spark: SparkSession, loadPrevMatrixFlag: String, outDir: String, currentMatrixDir: String) = {
        def findPrevMatrixFolder() = {
            val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

            val matrixFolders =
                if (outDir.startsWith(localFileSystemPrefix))
                    new File(outDir.substring(localFileSystemPrefix.length))
                        .listFiles
                        .filter(_.isDirectory)
                        .map(_.getPath)
                else
                    fs.listStatus(new Path(outDir))
                        .filter(_.isDirectory)
                        .map(_.getPath.toString)

            println("Previous matrix folders were found:")
            println(matrixFolders.mkString(" "))

            scala.util.Sorting.quickSort(matrixFolders)
            val sortedMatrixFolders = matrixFolders.filter(!_.contains(currentMatrixDir)).reverse
            val previousMatrixFolder = if (sortedMatrixFolders.isEmpty) "" else sortedMatrixFolders(0)
            previousMatrixFolder
        }

        if (loadPrevMatrixFlag == "1") {
            println("Yes, need to add users from previous matrix")

            val previousMatrixFolder = findPrevMatrixFolder()

            if (!previousMatrixFolder.isEmpty) {
                println(s"Path to previous matrix is not empty!")
                val previousMatrixFolderWithPrefix =
                    if (outDir.startsWith(localFileSystemPrefix))
                        localFileSystemPrefix + previousMatrixFolder
                    else
                        previousMatrixFolder
                println(s"Try to load from '$previousMatrixFolderWithPrefix' and append to '$outDir/$currentMatrixDir'")

                //load prev matrix
                spark
                    .read
                    .parquet(previousMatrixFolderWithPrefix)
                    .na.fill(0)
            }
            else {
                println("not found previous matrix data")
                spark.emptyDataFrame
            }
        }
        else {
            spark.emptyDataFrame
        }
    }

}
