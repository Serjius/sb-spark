package org.apache.spark.sql.npl

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

class MyDataSourceProvider extends StreamSourceProvider with Logging {
    override def sourceSchema(sqlContext: SQLContext,
                              schema: Option[StructType],
                              providerName: String,
                              parameters: Map[String, String]): (String, StructType) = {
        logInfo("sourceSchema call")
        logInfo(schema.toString)
        val first = "test"

        schema match {
            case Some(s) => (first, s)
            case None => throw new IllegalArgumentException("Schema S must be provide")
        }
    }

    override def createSource(sqlContext: SQLContext,
                              metadataPath: String,
                              schema: Option[StructType],
                              providerName: String,
                              parameters: Map[String, String]): Source = {

        logInfo("createSource call")
        logInfo(metadataPath)
        logInfo(schema.toString)
        logInfo(providerName)

        val step = parameters.get("step") match {
            case Some(s) => s.toInt
            case None => throw new IllegalArgumentException("Step must be provide")
        }

        schema match {
            case Some(s) => new MyDataSource(s, step)
            case None => throw new IllegalArgumentException("Schema C must be provide")
        }

    }
}

class MyDataSource (dataSchema: StructType, step: Int) extends Source with Logging {
    var i = 0

    override def schema: StructType = {
        logInfo("provider schema call")
        this.dataSchema
    }

    override def getOffset: Option[Offset] = {
        logInfo("provider getOffset call")
        //Option.empty[Offset]
        val currentOffset = new MyOffset(i)
        i += 1
        Some(currentOffset)
    }

    override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        logInfo("provider getBatch call")
        logInfo(s"Offsets: start=$start, end=$end")

        val startingOffset = start match {
            case Some(x) => x.json().toInt * step + 1
            case None => 0
        }
        val endingOffset = end.json.toInt * step

        //SparkSession.active.range(0, 10).toDF()
        val spark = SparkSession.active
        val sparkContext = spark.sparkContext
        //TODO SQL to create RDD
        val catalystRows: RDD[InternalRow] = sparkContext.parallelize(startingOffset to endingOffset).map {
            x => InternalRow.fromSeq(Seq(x.toLong, UTF8String.fromString("hello world")))
        }

        val isStreaming = true

        val df = spark.internalCreateDataFrame(catalystRows, this.dataSchema, isStreaming)

        df
    }

    override def stop(): Unit = {
        logInfo("provider stop call")
        Unit
    }
}

class MyOffset(value: Int) extends Offset {
    override def json(): String = {
        value.toString
    }
}