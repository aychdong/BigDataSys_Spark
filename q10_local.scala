import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.udf
import java.io._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

object wiki{
def main(args: Array[String]): Unit={
    //receiver
    val spark = SparkSession
        .builder
        .appName("StructuredStreamingReceiver")
        .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
        .format("text")
       .load("file:///users/dongchen/socket")

    val convert1 = udf((x: String) => x.substring(1, (x.lastIndexOf(","))))
    val convert2 = udf((x: String) => x.substring((x.lastIndexOf(",")+1), (x.length-1)))

    val lines1 = lines.withColumn("split0", convert1(lines("value")))
    val lines2 = lines1.withColumn("split1", convert2(lines("value")))
   val new_lines = lines2.select("split0", "split1")
    val newnew_lines = new_lines.filter($"split1" > "0.5")

    val query = newnew_lines.writeStream
        .format("csv")
        .option("delimiter","\t")
        .option("checkpointLocation", "hdfs://c220g2-011316.wisc.cloudlab.us:8020/checkpoint")
        .option("path", "file:///users/dongchen/receive_res")
        .start()

    query.awaitTermination()

}
}
