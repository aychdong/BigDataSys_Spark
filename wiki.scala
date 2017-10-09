import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object wiki{
def main(args: Array[String]): Unit={
val spark_master_url = "spark://c220g1-030818.wisc.cloudlab.us:7077"
val username = "dongchen"

val config = new SparkConf().setAppName("wiki").setMaster(spark_master_url)
val sc = new SparkContext(config)
val sqlContext = new SQLContext(sc)
val df = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "page").load("/enwiki-20110115-pages-articles_whole.xml")
df.printSchema()
val title_text = df.select("title", "revision.text._VALUE")
val text = title_text.select("_VALUE").toDF()
//val convert = udf[String](x => x.toLowerCase())

//val addByLit = udf((x: String) => x.toLowerCase())
val pattern = """\[\[(.*?)\]\]""".r
/*
val convert = udf((x: String) => pattern.findAllIn(x).toList.map(tmp=>tmp.substring(2, tmp.length-2)).filter(_.nonEmpty).map(tmp=>tmp.toLowerCase).filterNot{tmp=>tmp.contains('#')}.map(tmp=>tmp.split('|')(0)).filter(_.nonEmpty).filterNot{tmp=>(!tmp.startsWith("category:")) && tmp.contains(':')})
*/
//tmp
val convert = udf((x: String) => pattern.findAllIn(x).toList.map(tmp=>tmp.substring(2, tmp.length-2)).filter(_.nonEmpty).map(tmp=>tmp.toLowerCase).filterNot{tmp=>tmp.contains('#')}.map(tmp=>tmp.split('|')(0)).filter(_.nonEmpty).filterNot{tmp=>(!tmp.startsWith("category:")) && tmp.contains(':')})
val tmp = title_text.withColumn("newText", convert(col("_VALUE")))

val new_title_text = tmp.select("title", "newText")
val exploded = new_title_text.withColumn("newClo", explode($"newText"))
val final_res = exploded.select("title", "newClo")
final_res.write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save("/sb1.csv")
/*
def myFunc(str: String): List= {
    val pattern = """\[\[(.*?)\]\]""".r
    val x = pattern.findAllIn(str).toList.map(tmp=>tmp.substring(2, tmp.length-2)).map(tmp=>tmp.toLowerCase).filterNot{tmp=>tmp.contains('#')}.filterNot{tmp=>(!tmp.startsWith("category:")) && tmp.contains(':')}
    val list_with = x.filter{tmp=>tmp.contains("|")}
    val list_without = x.filterNot{tmp=>tmp.contains("|")}
    val list_with_res = list_with.map(tmp=>tmp.substring(0, tmp.indexOf("|")))
    val res = List.concat(list_with_res, list_without)
    return res
}
*/

/*
val x = pattern.findAllIn(text).toList.map(tmp=>tmp.substring(2, tmp.length-2)).map(tmp=>tmp.toLowerCase).filterNot{tmp=>tmp.contains('#')}.filterNot{tmp=>(!tmp.startsWith("category:")) && tmp.contains(':')}
val list_with = x.filter{tmp=>tmp.contains("|")}
val list_without = x.filterNot{tmp=>tmp.contains("|")}
val list_with_res = list_with.map(tmp=>tmp.substring(0, tmp.indexOf("|")))
val res = List.concat(list_with_res, list_without)
*/
}
}
