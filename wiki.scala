import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object wiki{
def main(): Unit= {
val sqlContext = new SQLContext(sc)
val df = sqlContext.read
        .format("com.databricks.spark.xml")
        .option("rowTag", "title")
        .load("/test_shell.xml")
df.printSchema()
}
}

