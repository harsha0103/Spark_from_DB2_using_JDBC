
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test_Data {

  def main(args: Array[String]) {
    val t1 = System.nanoTime
    print(t1 / 1e9d)
    var jdbcUsername = "-"
    var jdbcPassword = "-"
    val jdbcHostname = "-"
    val jdbcPort = 50000
    val jdbcDatabase = "-"
    // Create the JDBC URL without passing in the user and password parameters.
    val jdbcUrl = s"jdbc:db2://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"
    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")
    val driverClass = "com.ibm.db2.jcc.DB2Driver"
    connectionProperties.setProperty("Driver", driverClass)

    // Start Code


    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local[*]").getOrCreate()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    //getting the Codes
    spark.read.option("header", true).csv("/Users/harsha.kulluru/Documents/Metrics_METRIC_VALUESET_MAPPING_201803261104.csv").createOrReplaceTempView("Temp1")
    val CPT = spark.sqlContext.sql("""SELECT trim(DICT_CD) AS DICT_CD FROM Temp1 WHERE VALUESET_CD LIKE "%CG-30%" AND DICT_NM LIKE "%CPT%" """)
    val test123 = spark.read.option("header", true).csv("/Users/harsha.kulluru/Documents/Metrics_METRIC_VALUESET_MAPPING_201803261104.csv")
    test123.show()
    val CPT_Codes = CPT.select("DICT_CD").rdd.map(r => r(0)).collect().toList
    CPT_Codes.foreach(println)

    val REV = spark.sqlContext.sql("""SELECT trim(DICT_CD) AS DICT_CD FROM Temp1 WHERE VALUESET_CD LIKE "%CG-30%" AND DICT_NM LIKE "%UBREV%" """)
    val REV_Codes = REV.select("DICT_CD").rdd.map(r => r(0)).collect().toList
    REV_Codes.foreach(println)

    val POS = spark.sqlContext.sql("""SELECT trim(DICT_CD) AS DICT_CD FROM Temp1 WHERE VALUESET_CD LIKE "%CG-30%" AND DICT_NM LIKE "%POS%" """)
    val POS_Codes = POS.select("DICT_CD").rdd.map(r => r(0)).collect().toList
    POS_Codes.foreach(println)

    //getting the Tables

    val exat_Claim = spark.read.option("header", true).jdbc(jdbcUrl, "MENDATA.CLM_SVC_FCT", connectionProperties)
    val df2 = exat_Claim.filter(exat_Claim("PCD_CD").isin(CPT_Codes: _*)).filter(exat_Claim("REV_CD").isin(REV_Codes: _*)).filter(exat_Claim("PLC_OF_SVC_CD").isin(POS_Codes: _*))
    val df3 = df2.select("MBR_DK", "SVC_TYPE_CD", "PCD_CD", "REV_CD", "SVC_TYPE_CD_VAL", "PLC_OF_SVC_CD", "PLC_OF_SVC_CD_VAL", "SVC_FROM_DT", "SVC_TO_DT", "ADMIT_DT",
      "READMIT_WTN_30_DAY_IND", "PRCTNR_DK", "FCLTY_DK")
    df3.show()

    val exat_pract_dk = spark.read.option("header", true).jdbc(jdbcUrl, "mendata.prctnr_dim", connectionProperties)
    val prct_dk = exat_pract_dk.select("PRCTNR_DK", "FST_NM", "MDL_NM", "CITY_CD", "LAST_NM", "FULL_NM", "STATE_CD_VAL", "ZIP").cache()
    prct_dk.show()

    val exat_Facility_dk = spark.read.jdbc(jdbcUrl, "mendata.FCLTY_DIM", connectionProperties)
    val Facility_dk = exat_Facility_dk.select("FCLTY_DK", "ADR_LINE_2", "ADR_LINE_3", "CNTRY_CD_VAL", "CNTY_CD_VAL", "CITY_CD_VAL", "STATE_CD_VAL", "ZIP_CD").cache()

    Facility_dk.show()

    // table joins
    val doin = df3.join(prct_dk, Seq("PRCTNR_DK"))
    doin.show()
    val sq2 = doin.join(Facility_dk, Seq("FCLTY_DK"))
    print(sq2.count())
    sq2.createOrReplaceTempView("temp5")

    //last 30 days

    val df5 = spark.sqlContext.sql(
      """SELECT *,TO_DATE("2017-10-25") as Start_Date, DATEDIFF(TO_DATE("2017-10-25"), TO_DATE(SVC_FROM_DT)) as Number_Of_Times from temp5
        where DATEDIFF(TO_DATE("2017-10-25"), TO_DATE(SVC_FROM_DT)) < 30 and DATEDIFF(TO_DATE("2017-10-25"), TO_DATE(SVC_FROM_DT)) > 0""".stripMargin)
    df5.show()
    df5.printSchema()
    df5.createOrReplaceTempView("temp6")

    val df6 = spark.sqlContext.sql("""SELECT  MBR_DK,REV_CD,COUNT(*) as Count from temp6 GROUP by MBR_DK, REV_CD HAVING COUNT(*) > 1""".stripMargin)
    df6.createOrReplaceTempView("temp7")
    df6.show()


    if (df6.count() != 0) {
      val Member_DK = df6.select("MBR_DK").rdd.map(r => r(0)).collect().toList
      Member_DK.foreach(println)
      val df7 = df5.filter(df5("MBR_DK").isin(Member_DK: _*))
      df7.show()
      //        val df8=df7.filter(df7("PRCTNR_DK").notEqual(999999999)).filter(df7("FCLTY_DK").notEqual(999999999))
      //        df8.show()
    }
    else {
      print("There are no unique customers")
    }
    val duration = (System.nanoTime - t1) / 1e9d
    print(duration)
  }


}
