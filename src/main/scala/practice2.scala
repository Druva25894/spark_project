import org.apache.commons.lang3.BooleanUtils.and
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object practice2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("practice2")
      .master("local")
      .getOrCreate()

    val col = Seq("Year_of_study", "course", "Age", "appheight", "appweight", "Kcse", "Yr_joincampus", "sitkcse", "expense_semister", "accm_expense")
    val data = Seq(("Second Year", "MATHEMATICS", "20", "152", "80", "2016", "2018", "Central", "8744", "6043"), ("Second Year", "MATHEMATICS", "20", "152", "80", "2017", "2019", "Central", "8754", "6443"))

    val datardd = spark.sparkContext.parallelize(data)
 
    val fildatarddd = datardd.filter(x=>x.(20))


  }
}
