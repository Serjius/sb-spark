package org.apache.spark.sql.npl

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{functions => f}


class MyDataSinkProvider extends StreamSinkProvider with Logging {
    override def createSink(sqlContext: SQLContext,
                            parameters: Map[String, String],
                            partitionColumns: Seq[String],
                            outputMode: OutputMode): Sink = {
        logInfo("sink createSink call")
        logInfo(outputMode.toString)
        new MyDataSink
    }
}

class MyDataSink extends Sink with Logging {
    override def addBatch(batchId: Long, data: DataFrame): Unit = {
        logInfo("sink addBatch call")

        val rdd = data.queryExecution.executedPlan.execute()

        //rdd.foreachPartition(x => println(x.length))

        val df = SparkSession.active.internalCreateDataFrame(rdd, data.schema, isStreaming = false)
        df
            .withColumn("foo", f.upper(f.col("foo")))
            .show(20, truncate = false)


    }
}