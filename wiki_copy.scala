import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.explode

object wiki{
def main(args: Array[String]): Unit={
val spark_master_url = "spark://c220g1-030627.wisc.cloudlab.us:7077"
val username = "dongchen"

val config = new SparkConf().setAppName("wiki").setMaster(spark_master_url)
val sc = new SparkContext(config)
val sqlContext = new SQLContext(sc)
val df = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "page").load("/enwiki-20110115-pages-articles1.xml")
val title_text_tmp = df.select("title", "revision.text._VALUE")
val title_text = title_text_tmp.filter("_VALUE is not null")
val pattern = """\[\[(.*?)\]\]""".r
val convert = udf((x: String) => pattern.findAllIn(x).toList.map(tmp=>tmp.substring(2, tmp.length-2)).filter(_.nonEmpty).map(tmp=>tmp.toLowerCase).map(tmp=>tmp.split("""\|""", -1)(0)).filter(_.nonEmpty).filterNot{tmp=>tmp.contains('#')}.filterNot{tmp=>(!tmp.startsWith("category:")) && tmp.contains(':')})
val tmp = title_text.withColumn("newText", convert(title_text("_VALUE")))
val new_title_text = tmp.select("title", "newText")
val exploded = new_title_text.withColumn("newClo", explode(col("newText")))
val final_res = exploded.select("title", "newClo")
final_res.write.format("com.databricks.spark.csv").option("header", "false").option("delimiter","\t").mode("overwrite").save("/sb1.csv")
}
}

