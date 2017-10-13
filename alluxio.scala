import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.udf
import java.io._

object wiki{
def main(args: Array[String]): Unit={
        val spark_master_url = "spark://c220g1-030627.wisc.cloudlab.us:7077"
val username = "dongchen"

val config = new SparkConf().setAppName("wiki").setMaster(spark_master_url)
val sc = new SparkContext(config)
val sqlContext = new SQLContext(sc)
val df = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "page").load("/enwiki-20110115-pages-articles_whole.xml")
val title_text_tmp = df.select("title", "revision.text._VALUE")
//val text = title_text.select("_VALUE").toDF()
val title_text = title_text_tmp.filter("_VALUE is not null")
val pattern = """\[\[(.*?)\]\]""".r
val convert = udf((x: String) => pattern.findAllIn(x).toList.map(tmp=>tmp.substring(2, tmp.length-2)).filter(_.nonEmpty).map(tmp=>tmp.toLowerCase).map(tmp=>tmp.split("""\|""", -1)(0)).filter(_.nonEmpty).filterNot{tmp=>tmp.contains('#')}.filterNot{tmp=>(!tmp.startsWith("category:")) && tmp.contains(':')})
val tmp = title_text.withColumn("newText", convert(title_text("_VALUE")))
val new_title_text = tmp.select("title", "newText")
val exploded = new_title_text.withColumn("newClo", explode(col("newText")))
val final_res = exploded.select("title", "newClo")
//write
final_res.write.format("com.databricks.spark.csv").option("header", "false").option("delimiter","\t").mode("overwrite").save("alluxio://node-0.xiaoyu-a1.michigan-bigdata-pg0.wisc.cloudlab.us:19998/alluxio_whole.csv")
        val file = sc.textFile("alluxio://node-0.xiaoyu-a1.michigan-bigdata-pg0.wisc.cloudlab.us:19998/alluxio_whole.csv")
    val links = file.filter{tmp => tmp.contains("\t") && (tmp.split("\t").length > 1)}.map{ s =>
        val parts = s.split("\t")
        (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to 10) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val output = ranks.collect()

    val pw = new PrintWriter(new File("/users/dongchen/tmp/pageRank_alluxio.txt"))
    output.foreach(tup => pw.write(tup._1 + "\t" + tup._2 + "\n"))
}
}
               
