package org.apache.spark.sql.npl

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode


class MyDataSinkProvider extends StreamSinkProvider with Logging{
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
        Unit
    }
}