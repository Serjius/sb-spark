package com.npl

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.{LongType, StructField, StructType}

class MyDataSource extends StreamSourceProvider with Logging {
    override def sourceSchema(sqlContext: SQLContext,
                              schema: Option[StructType],
                              providerName: String,
                              parameters: Map[String, String]): (String, StructType) = {
        logInfo("sourceSchema call")
        val first = "test"
        val second = StructType(StructField("id", LongType) :: Nil)
        (first, second)
    }

    override def createSource(sqlContext: SQLContext,
                              metadataPath: String,
                              schema: Option[StructType],
                              providerName: String,
                              parameters: Map[String, String]): Source = {

        logInfo("createSource call")
        new MyDataSourceProvider
    }
}

class MyDataSourceProvider extends Source with Logging {
    override def schema: StructType = {
        logInfo("provider schema call")
        StructType(StructField("id", LongType) :: Nil)
    }

    override def getOffset: Option[Offset] = {
        logInfo("provider getOffset call")
        Option.empty[Offset]
    }

    override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        logInfo("provider getBatch call")
        SparkSession.active.range(0, 10).toDF()
    }

    override def stop(): Unit = {
        logInfo("provider stop call")
        Unit
    }
}