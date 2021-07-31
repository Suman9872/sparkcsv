package guru.learning

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._



object Udf extends Serializable {

  @transient lazy val logger: Logger =Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    if(args.length == 0){

      logger.error("Usage:Udf filename")

      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("untitled1")
      .master("local[3]")
      .getOrCreate()

    val flightTimeDF1 = spark.read.json("Data/d1")

    val flightTimeDF2 = spark.read.json("Data/d2")


     import spark.sql

    sql("CREATE DATABASE IF NOT EXISTS MY_DB")
    sql("USE MY_DB")

    flightTimeDF1.coalesce(1).write
      .bucketBy(3,"id")
      .mode(SaveMode.Overwrite)
      .saveAsTable("MY_DB.flight_data1")

    flightTimeDF2.coalesce(1).write
      .bucketBy(3,"id")
      .mode(SaveMode.Overwrite)
      .saveAsTable("MY_DB.flight_data2")

    val df3 = spark.read.table("MY_DB.flight_data1")
    val df4 = spark.read.table("MY_DB.flight_data2")

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

    val joinExpr = df3.col("id")===df4.col("id")
    val joinDF = df3.join(df4,joinExpr,"inner")

    joinDF.foreach(_ => ())



/*

    val ordersList = List(
      ("01", "02", 350, 1),
      ("01", "04", 580, 1),
      ("01", "07", 320, 2),
      ("02", "03", 450, 1),
      ("02", "06", 220, 1),
      ("03", "01", 195, 1),
      ("04", "09", 270, 3),
      ("04", "08", 410, 2),
      ("05", "02", 350, 1)
    )
    val orderDF = spark.createDataFrame(ordersList).toDF("order_id", "prod_id", "unit_price", "qty")

    val productList = List(
      ("01", "Scroll Mouse", 250, 20),
      ("02", "Optical Mouse", 350, 20),
      ("03", "Wireless Mouse", 450, 50),
      ("04", "Wireless Keyboard", 580, 50),
      ("05", "Standard Keyboard", 360, 10),
      ("06", "16 GB Flash Storage", 240, 100),
      ("07", "32 GB Flash Storage", 320, 50),
      ("08", "64 GB Flash Storage", 430, 25)
    )


    val productDF = spark.createDataFrame(productList).toDF("prod_id", "prod_name", "list_price", "qty")

    val productsRenamedDF = productDF.withColumnRenamed("qty","reorder_qty")

    val joinExpr =  orderDF.col("prod_id")=== productDF.col("prod_id")

    orderDF.join(productsRenamedDF,joinExpr,"left")
      .drop(productsRenamedDF.col("prod_id"))
      .select("order_id","prod_id","prod_name","unit_price","list_price","qty")
      .withColumn("prod_name",expr("coalesce(prod_name,prod_id)"))
      .withColumn("list_price",expr("coalesce(list_price,unit_price)"))
      .sort("order_id")
      .show()

*/


/*
    val invoiceDF = spark.read
      .format("parquet")
      .load("Data/")


    val runningWindowDF = Window.partitionBy("Country")
      .orderBy("WeekNumber")
      .rowsBetween(Window.unboundedPreceding,Window.currentRow)

    invoiceDF.withColumn("RunningTotal",sum("InvoiceValue").over(runningWindowDF)).show()


*/

/*
    val NumInvoices = countDistinct("InvoiceNo").as("NumInvoices")
    val TotalQuantity = sum("Quantity").as("TotalQuantity")
    val InvoiceValue = expr("round(sum(Quantity*UnitPrice),2) as InvoiceValue")

    val exSummaryDF = invoiceDF
      .withColumn("Invoice",to_date(col("InvoiceDate"),"dd-MM-yyyy H.mm"))
      .where("year(InvoiceDate)==2010")
      .withColumn("WeekNumber",weekofyear(col("InvoiceDate")))
      .groupBy("Country","WeekNumber")
      .agg(NumInvoices,TotalQuantity,InvoiceValue)

    exSummaryDF.coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .save("output")

    exSummaryDF.sort("Country","WeekNumber").show(20)

*/
/*
    invoiceDF.createTempView("sales")

    val summarySQL = spark.sql(
      """
        |SELECT Country, InvoiceNo,
        |sum(Quantity) as TotalQuantity,
        |round(sum(Quantity*UnitPrice),2) as InvoiceValue
        |FROM sales
        |GROUP BY Country,InvoiceNo
        |""".stripMargin)

    summarySQL.show()

    val summaryDF = invoiceDF.groupBy("Country", "InvoiceNo")
      .agg(sum("Quantity").as("TotalQuantity"),
        round(sum(expr("Quantity*UnitPrice")),2).as("InvoiceValue"),
        expr("round(sum(Quantity*UnitPrice),2) as InvoiceValue")
      )
summaryDF.show()  */
/*
    invoiceDF.select(
      count("*").as("Count *"),
      sum("Quantity").as("TotalQuantity"),
      avg("UnitPrice").as("AvgPrice"),
      countDistinct("InvoiceNo").as("CountDistinct")
    ).show()


    invoiceDF.selectExpr(
      "count(1) as`count 1`",
      "count(StockCode) as `count field`",
      "sum(Quantity) as  TotalQuantity",
      "avg(UnitPrice) as AvgPrice"

    ).show()
*/
/*
    val dataList = List(
      ("Ravi",28,1,2002),
      ("Abdul",23,5,81),
      ("Ravi",12,12,6),
      ("Ravi",7,8,63),
      ("Ravi",23,5,81)
    )

    val rawDF = spark.createDataFrame(dataList).toDF("name","day","month","year").repartition(3)

    val finalDF = rawDF.withColumn("id",monotonically_increasing_id)
      .withColumn("day",col("day").cast(IntegerType))
      .withColumn("month",col("month").cast(IntegerType))
      .withColumn("year",col("year").cast(IntegerType))
      .withColumn("year",
        when(col("year")<21, col("year")+2000)
      when(col("year")< 100, col("year")+1900)
      otherwise(col("year")))
      .withColumn("dob",to_date(expr("concat(day,'/',month,'/',year)"),"d/M/y"))
      .drop("day","month","year")
      .dropDuplicates("name","dob")
      .sort(expr("dob desc"))
*/
    /*

      .withColumn("year",expr(
        """
          |case when year <21 then year +2000
          |when year <100 then year +1900
          |else year
          |end
          |""".stripMargin))
*/





/*
    val surveyDF = spark.read
      .format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load(args(0))


    surveyDF.show(10,false)

    import org.apache.spark.sql.functions._
    val parseGenderUDF = udf(parseGender(_:String):String)
    spark.catalog.listFunctions().filter(r => r.name == "parseGenderUDF").show()

    val surveyDF2 =surveyDF.withColumn("Gender",parseGenderUDF(col("Gender")))
    surveyDF2.show(10,false)

    spark.udf.register("parseGenderUDF",parseGender(_:String):String)
    spark.catalog.listFunctions().filter(r => r.name == "parseGenderUDF").show()
    val surveyDF3 = surveyDF.withColumn("Gender",exp("parseGenderUDF(Gender)"))
    surveyDF3.show(10,false)

  }
  def parseGender(s:String):String ={

    val femalePattern = "^f$|f.m|w.m".r
    val malePattern = "^m$|ma|m.1".r

    if (femalePattern.findFirstIn(s.toLowerCase).nonEmpty)"Female"
    else if(malePattern.findFirstIn(s.toLowerCase).nonEmpty)"Male"
      else "Unknown"   */
  }

}
