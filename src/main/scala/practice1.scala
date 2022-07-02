import org.apache.commons.lang3.BooleanUtils.and
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
object practice1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("practice1")
      .master("local")
      .getOrCreate()

    val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("C:\\Users\\Nelluri\\OneDrive\\Desktop\\WHO-COVID-19-global-data.csv")
    // df.show(20)
    // df.printSchema()//
    //df.groupBy("Country").sum("Cumulative_deaths").show(false)//

    val df2 = Window.partitionBy("Country").orderBy(col("Cumulative_deaths").desc)
    df.withColumn("Rank", ntile(2).over(df2)).where("Rank=2 and Country=Chad").show(1000,false)


  }
}