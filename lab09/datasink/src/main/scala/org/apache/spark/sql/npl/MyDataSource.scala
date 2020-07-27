package org.apache.spark.sql.npl

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

class MyDataSource extends StreamSourceProvider with Logging {
    override def sourceSchema(sqlContext: SQLContext,
                              schema: Option[StructType],
                              providerName: String,
                              parameters: Map[String, String]): (String, StructType) = {
        logInfo("sourceSchema call")
        val first = "test"
        val second = StructType(
            StructField("id", LongType) ::
                StructField("foo", StringType) ::
                Nil
        )
        (first, second)
    }

    override def createSource(sqlContext: SQLContext,
                              metadataPath: String,
                              schema: Option[StructType],
                              providerName: String,
                              parameters: Map[String, String]): Source = {

        logInfo("createSource call")
        logInfo(metadataPath)
        new MyDataSourceProvider
    }
}

class MyDataSourceProvider extends Source with Logging {
    override def schema: StructType = {
        logInfo("provider schema call")
        StructType(
            StructField("id", LongType) ::
                StructField("foo", StringType) ::
                Nil
        )
    }

    override def getOffset: Option[Offset] = {
        logInfo("provider getOffset call")
        //Option.empty[Offset]
        Some(new MyOffset)
    }

    override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        logInfo("provider getBatch call")
        //SparkSession.active.range(0, 10).toDF()
        val spark = SparkSession.active
        val sparkContext = spark.sparkContext
        val catalystRows: RDD[InternalRow] = sparkContext.parallelize(0 to 10).map {
            x => InternalRow.fromSeq(Seq(x.toLong, UTF8String.fromString("hello world")))
        }
        val schema = StructType(
            StructField("id", LongType) ::
            StructField("foo", StringType) ::
            Nil
        )
        val isStreaming = true

        val df = spark.internalCreateDataFrame(catalystRows, schema, isStreaming)

        df
    }

    override def stop(): Unit = {
        logInfo("provider stop call")
        Unit
    }
}

class MyOffset extends Offset {
    override def json(): String = {
        "test"
    }
}