import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.udf
import java.io._

object pageRank{
def main(args: Array[String]): Unit={
	val spark_master_url = "spark://c220g1-030627.wisc.cloudlab.us:7077"
	val username = "dongchen"

	val config = new SparkConf().setAppName("pageRank").setMaster(spark_master_url)
	val sc = new SparkContext(config)

	val file = sc.textFile("/sb_whole.csv")
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
    
    val pw = new PrintWriter(new File("/users/dongchen/tmp/pageRank.txt"))
    output.foreach(tup => pw.write(tup._1 + "\t" + tup._2 + "\n"))
}
}
