package sort

import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SortByAmount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SortByAmount").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)

    //val rdd: RDD[String] = sc.textFile("C:\\Users\\Lenovo\\Desktop\\新建文件夹")

    val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import ss.implicits._

    val raw: DataFrame = ss.read.format("csv")
      .option("delimiter", ",")
      .option("encoding", "utf-8")
      .option("enforceSchema", false)
      .option("header", "true")
      .option("quote", "'")
      .option("nullValue", "\\N")
      .option("ignoreLeadingWhiteSpace", false)
      .option("ignoreTrailingWhiteSpace", false)
      .option("multiline", "true")
      .load("C:\\Users\\Lenovo\\Desktop\\新建文件夹\\test")
      .withColumnRenamed("序号", "id")
      .withColumnRenamed("项目单位", "project_unit")
      .withColumnRenamed("项目名称", "project_name")
      .withColumnRenamed("投资额", "investment_amount")
      .withColumnRenamed("责任单位", "responsible_unit")
      .withColumnRenamed("项目类型", "project_type")
      .withColumnRenamed("入库时间", "date")

    //raw.show()

    raw.createOrReplaceTempView("table")

    val currentMonth: String = String.valueOf(Calendar.getInstance().get(Calendar.MONTH) + 1)

    val sortByAmount: DataFrame = ss.sql(
      """
        |select
        |row_number() over(order by t1.total_amount desc) as rn,
        |t1.*
        |from
        |(
        |     select
        |     responsible_unit,
        |     count(1) total_project,
        |     sum(investment_amount) total_amount,
        |     sum(if(date = concat('拟入',month(current_date),'月库'),1,0)) current_month_total_project,
        |     sum(if(date = concat('拟入',month(current_date),'月库'),investment_amount,0)) current_month_total_amount
        |     from table
        |     group by responsible_unit
        |) t1
        |where responsible_unit != 'null'
        |order by t1.total_amount desc
      """.stripMargin)

    val rdd: RDD[Row] = sortByAmount.rdd

    rdd.foreach(t => {
      val id: String = t.get(0).toString
      val responsibleUnit: String = t.get(1).toString
      val totalProject: String = t.get(2).toString
      val totalAmount: String = t.get(3).toString
      val currentMonthTotalProject: String = t.get(4).toString
      val currentMonthTotalAmount: String = t.get(5).toString

      println(id + "." + responsibleUnit + "拟入库" + totalProject + "个，" + totalAmount + "万元，（" + currentMonth + "月新增" + currentMonthTotalProject + "个，" + currentMonthTotalAmount + "万元）")
    })

    sc.stop()
  }
}
