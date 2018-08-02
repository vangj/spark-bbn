package com.github.vangj.dp.test

import com.opencsv.CSVParser
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

object TestParquetCode {

  //~/development/spark-1.6.0-bin-hadoop2.6/bin/spark-shell --master spark://dqbox:7077 --packages com.databricks:spark-csv_2.10:1.5.0
  case class Options(id:String, hasHeaders: Boolean, delimiter: Char, quote: Char, escape: Char)

  def getHeaders(delimiter: Char, quote: Char, escape: Char, data: RDD[String]): List[String] = {
    (new CSVParser(delimiter, quote, escape))
      .parseLine(data.first())
      .toList
  }

  def getSchema(headers: List[String]): StructType = {
    StructType(headers.map(field => StructField(field, StringType, nullable = true)))
  }

  def getDataFrame(path: String, options: Options, sc: SparkContext, sqlContext: SQLContext): DataFrame = {
    sc.textFile(path).first()
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", if (options.hasHeaders) "true" else "false")
      .option("inferSchema", "false")
      .load("")
  }

  def testSparkCsv(sc: SparkContext, sqlContext: SQLContext): Unit = {
    val df1 = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("hdfs://dqbox/dataqwiki/account/root/178752a47dc14cf3bc266f948ae7d1bc/data-withheaders.csv")

    val df2 = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load("hdfs://dqbox/dataqwiki/account/root/d5b800f4610d4b1d985343973710ff17/data-noheaders.csv")

    val df3 = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false")
      .load("hdfs://dqbox/dataqwiki/account/root/d5b800f4610d4b1d985343973710ff17/data-noheaders.csv")
  }

  def testCombinations(sc: SparkContext, sqlContext: SQLContext): Unit = {
    val tdf1 = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      .load("hdfs://dqbox/jee/headers_mixed.csv")

    val tdf2 = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .load("hdfs://dqbox/jee/headers_nomixed.csv")

    val tdf3 = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false")
      .load("hdfs://dqbox/jee/noheaders_mixed.csv")

    val tdf4 = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false")
      .load("hdfs://dqbox/jee/noheaders_nomixed.csv")
  }

  def test(sc: SparkContext, sqlContext: SQLContext): Unit = {
    val rdd = sc.textFile("hdfs://dqbox/dataqwiki/account/root/178752a47dc14cf3bc266f948ae7d1bc/data-withheaders.csv")
    val headers = rdd.first()
    val schema =
      StructType(
        headers.split(",")
          .map(fieldName => StructField(fieldName, StringType, nullable = true)))

    val data =
      rdd
        .filter(line => !headers.equals(line))
        .map(_.split(","))
        .map(arr => Row.fromSeq(arr))

    val df = sqlContext.createDataFrame(data, schema)
    df.createOrReplaceTempView("data")
    df.cache()

    sqlContext.sql("select count(*) as total from data where n1='false'").collect()(0)
    sqlContext.sql("select count(*) as total from data where n1='true'").collect()(0)
    sqlContext.sql("select count(*) as total from data where n1='true'").collect()(0).get(0)
    sqlContext.sql("select count(*) as total from data where n1='true'").collect().head.getLong(0)

    var result = sqlContext.sql("select count(*) as total from data where n1='true'").collect().head

    df.write.parquet("hdfs://dqbox/dataqwiki/account/root/178752a47dc14cf3bc266f948ae7d1bc/parquet")

    val dfp = sqlContext.read.parquet("hdfs://dqbox/dataqwiki/account/root/178752a47dc14cf3bc266f948ae7d1bc/parquet")

    dfp.where("n1='true'").count()
  }

  def testGetProfile(sc: SparkContext, sqlContext: SQLContext): Unit = {
    val df = sqlContext.read.parquet("hdfs://dqbox/dataqwiki/account/root/dataset/b407871da87346edac09cfd9927b8ce0/parquet").cache
    df.createOrReplaceTempView("data")

    val p1 = sqlContext.sql("select n1 from data group by n1")
    p1.show()

    val p2 = df.groupBy("n1").count()
    p2.show()

    val profiles =
      df.columns.map(col => {
        val values = df.groupBy(col).count().collect().map(_.getString(0)).toList
        (col, values)
      })

    //NPE throw cannot operate on RDD with another RDD operation
    //http://stackoverflow.com/questions/23793117/nullpointerexception-in-scala-spark-appears-to-be-caused-be-collection-type
//    val profiles = sc.parallelize(df.columns).map(col => {
//      val values = df.groupBy(col).count().map(_.getString(0)).collect().toList
//      (col, values)
//    }).collect()

    df.columns
        .filter(!_.equals("id"))
        .map(col => {
          val values = df.groupBy(col)
            .count()
            .collect()
            .map(v => (v.getString(0), v.getLong(1)))
            .toList
          (col, values)
        })
  }

  def testNested(sc: SparkContext, sqlContext: SQLContext, spark: SparkSession): Unit = {
    import spark.implicits._
    val df = Seq(("t","t"), ("t", "f"), ("f", "t"), ("f", "f")).toDF("n1", "n2")
    df.printSchema
    df.show

    val filters = List("n1='false'", "n1='true'").par

    val counts = filters.map(df.filter(_).count).toList
  }

  def testMultipleColsApproach(sc: SparkContext, sqlContext: SQLContext, spark: SparkSession): Unit = {
    import spark.implicits._
    val df = Seq(("t","t"), ("t", "f"), ("f", "t"), ("f", "f")).toDF("n1", "n2")

    import org.apache.spark.sql.functions.udf
    val fudf = udf( (x: String) => if (x.equals("t")) 1 else 0)

    var df2 = df
    for (i <- 0 until 10000) {
      df2 = df2.withColumn("filter"+i, fudf($"n1"))
    }

    var index = 0
    var df3 = df.withColumn("filter1", fudf($"n1"))
      .withColumn("filter2", fudf($"n1"))
      .withColumn("filter3", fudf($"n1"))
      .withColumn("filter4", fudf($"n1"))
      .withColumn("filter5", fudf($"n1"))
      .withColumn("filter6", fudf($"n1"))
      .withColumn("filter7", fudf($"n1"))
      .withColumn("filter8", fudf($"n1"))
      .withColumn("filter9", fudf($"n1"))
      .withColumn("filter10", fudf($"n1"))
      .withColumn("filter11", fudf($"n1"))
      .withColumn("filter12", fudf($"n1"))
      .withColumn("filter13", fudf($"n1"))
      .withColumn("filter14", fudf($"n1"))
      .withColumn("filter15", fudf($"n1"))
      .withColumn("filter16", fudf($"n1"))
      .withColumn("filter17", fudf($"n1"))
  }

  def testColsApproach(sc: SparkContext, sqlContext: SQLContext, spark: SparkSession): Unit = {
    import spark.implicits._
    val df = Seq(("t","t"), ("t", "f"), ("f", "t"), ("f", "f")).toDF("n1", "n2")

    def filter(v1: Seq[Any], v2: Seq[String]): Int = {
      for (i <- 0 until v1.length) {
        if (!v1(i).equals(v2(i))) {
          return 0
        }
      }
      return 1
    }

    import org.apache.spark.sql.functions.udf
    val fudf = udf(filter(_: Seq[Any], _: Seq[String]))

//    df.withColumn("filter1", fudf(Seq($"n1"), Seq("t"))).show()
  }

  def testColsApproach2(sc: SparkContext, sqlContext: SQLContext, spark: SparkSession): Unit = {
    import spark.implicits._
    val df = Seq(("t","t"), ("t", "f"), ("f", "t"), ("f", "f")).toDF("n1", "n2")

    def filter(v1: Seq[Any], v2: Seq[String]): Int = {
      for (i <- 0 until v1.length) {
        if (!v1(i).equals(v2(i))) {
          return 0
        }
      }
      return 1
    }

    import org.apache.spark.sql.functions.{array, col, lit, udf}
    val fudf = udf(filter(_: Seq[Any], _: Seq[String]))

    df.withColumn("filter1", fudf(array($"n1"), array(lit("t")))).show()
    df.withColumn("filter1", fudf(array(col("n1")), array(lit("t")))).show()
    df.withColumn("filter1", fudf(array($"n1", $"n2"), array(lit("t"), lit("t")))).show()

    df.createOrReplaceTempView("df")
    spark.sqlContext.udf.register("testfilter", (v1: Seq[Any], v2: Seq[String]) => if (v1.equals(v2)) 1 else 0)
    spark.sql("select testfilter(array(n1), array('t')) as f1 from df").show

    val sql =
      """
        |select
        | testfilter(array(n1), array('t')) as f1,
        | testfilter(array(n1,n2), array('t','t')) as f2,
        | testfilter(array(n1,n2), array('t','f')) as f3,
        | testfilter(array(n1,n2), array('f','t')) as f4,
        | testfilter(array(n1,n2), array('f','f')) as f5
        |from df
      """.stripMargin
    spark.sql(sql).show

    import org.apache.spark.sql.functions.sum
    spark.sql(sql).select(sum($"f1"), sum($"f2")).show

    val sumDf = spark.sql(sql)
    sumDf.createOrReplaceTempView("sumDf")
    val sql2 =
      """
        |select
        | sum(f1) as f1,
        | sum(f2) as f2,
        | sum(f3) as f3,
        | sum(f4) as f4,
        | sum(f5) as f5
        |from sumDf
      """.stripMargin
    spark.sql(sql2).show

    val sql3 =
      """
        |select
        | sum(f1) as f1,
        | sum(f2) as f2,
        | sum(f3) as f3,
        | sum(f4) as f4,
        | sum(f5) as f5
        |from
        |(
        |select
        | testfilter(array(n1), array('t')) as f1,
        | testfilter(array(n1,n2), array('t','t')) as f2,
        | testfilter(array(n1,n2), array('t','f')) as f3,
        | testfilter(array(n1,n2), array('f','t')) as f4,
        | testfilter(array(n1,n2), array('f','f')) as f5
        |from df
        |)
      """.stripMargin

    spark.sql(sql3).explain

    val max = 1000
    val sums = (1 to max).map(i => s"sum(f${i}) as f${i}").mkString(", ")
    val filters = (1 to max).map(i => s"testfilter(array(n1), array('t')) as f${i}").mkString(", ")
    val usql =
      s"""
         |select ${sums}
         |from (
         | select ${filters}
         | from df
         |)
       """.stripMargin
    spark.sql(usql).collect
  }

  def testColsApproach3(sc: SparkContext, sqlContext: SQLContext, spark: SparkSession): Unit = {
    import spark.implicits._
    val df = Seq(("t","t"), ("t", "f"), ("f", "t"), ("f", "f")).toDF("n1", "n2")

    def filter(v1: Seq[Any], v2: Seq[String]): Int = {
      for (i <- 0 until v1.length) {
        if (!v1(i).equals(v2(i))) {
          return 0
        }
      }
      return 1
    }

    import org.apache.spark.sql.functions.{col, sum, udf}
    val fudf = udf(filter(_: Seq[Any], _: Seq[String]))

    df.createOrReplaceTempView("df")
    spark.sqlContext.udf.register("testfilter", (v1: Seq[Any], v2: Seq[String]) => if (v1.equals(v2)) 1 else 0)

    val sql =
      """
        |select
        | testfilter(array(n1), array('t')) as f1,
        | testfilter(array(n1,n2), array('t','t')) as f2,
        | testfilter(array(n1,n2), array('t','f')) as f3,
        | testfilter(array(n1,n2), array('f','t')) as f4,
        | testfilter(array(n1,n2), array('f','f')) as f5
        |from df
      """.stripMargin

    val dfCounts = spark.sql(sql)
    val dfSums = dfCounts.select(dfCounts.columns.map(c => sum(col(c))): _*)
  }

  def testGenerateData(spark: SparkSession): Unit = {
    spark.sqlContext.udf.register("eqUdf", (v1: Seq[Any], v2: Seq[String]) => if (v1.equals(v2)) 1 else 0)

    val cols = 200
    val rows = 100000
    val numFilters = 100

    var df = createDataFrames(cols, rows, numFilters, spark)
  }

  def testGenerateWithParquet(spark: SparkSession): Unit = {
    val cols = 200
    val rows = 100000
    val numFilters = 300
    val path = "hdfs://dqbox/temp/data/000"
    saveDataAsParquet(cols, rows, path, spark)
    val df = getDataFrame(path, spark)
    val counts = computeCounts(cols, rows, numFilters, df, spark)
    val sums = computeSums(numFilters, counts)

    computeCounts(cols, rows, 300, df, spark).groupBy().sum().collect()
  }

  def createSql(cols: Int, numFilters: Int): String = {
    val sCols = Seq.range(0, numFilters)
      .map(i => (i, scala.util.Random.nextInt(5) + 1))
      .map(pair => {
        val i = pair._1
        val total = pair._2
        val colsIndexes = Seq.fill(total)(scala.util.Random.nextInt(cols))
        val arrCols = colsIndexes.map(index => s"c${index}").mkString(",")
        val arrVals = colsIndexes.map(index => if (scala.util.Random.nextDouble() > 0.5) "'t'" else "'f'").mkString(",")
        s"eqUdf(array(${arrCols}), array(${arrVals})) as f${i}"
      })
      .mkString(",")

    s"select ${sCols} from df"
  }

  def createDataFrame(cols: Int, rows: Int, spark: org.apache.spark.sql.SparkSession): org.apache.spark.sql.DataFrame = {
    val rdd = spark.sparkContext.makeRDD(
      Seq.fill(rows)(
        Seq.fill(cols)(scala.util.Random.nextDouble)
          .map(n => if (n > 0.5) "t" else "f"))
        .map(r => org.apache.spark.sql.Row(r:_*)))

    val schema = new org.apache.spark.sql.types.StructType(
      Seq.range(0, cols)
        .map(i => new org.apache.spark.sql.types.StructField(
          s"c${i}", org.apache.spark.sql.types.StringType, nullable = true)).toArray)

    val df = spark.createDataFrame(rdd, schema).cache
    df.createOrReplaceTempView("df")
    df
  }

  def createDataFrames(cols: Int, rows: Int, numFilters: Int, spark: org.apache.spark.sql.SparkSession): org.apache.spark.sql.DataFrame = {
    val df = createDataFrame(cols, rows, spark)
    val sql = createSql(cols, numFilters)

    import org.apache.spark.sql.functions.{col, sum}
    val dfCounts = spark.sql(sql)
    val dfSums = dfCounts.select(dfCounts.columns.map(c => sum(col(c))): _*)
      .toDF(Seq.range(0, cols).map(c => s"f${c}"): _*)

    dfSums
  }

  def saveDataAsParquet(cols: Int, rows: Int, path: String, spark: org.apache.spark.sql.SparkSession): Unit = {
    val rdd = spark.sparkContext.makeRDD(
      Seq.fill(rows)(
        Seq.fill(cols)(scala.util.Random.nextDouble)
          .map(n => if (n > 0.5) "t" else "f"))
        .map(r => org.apache.spark.sql.Row(r:_*)))

    val schema = new org.apache.spark.sql.types.StructType(
      Seq.range(0, cols)
        .map(i => new org.apache.spark.sql.types.StructField(
          s"c${i}", org.apache.spark.sql.types.StringType, nullable = true)).toArray)

    val df = spark.createDataFrame(rdd, schema)
    df.write.parquet(path)
  }

  def getDataFrame(path: String, spark: org.apache.spark.sql.SparkSession): org.apache.spark.sql.DataFrame = {
    spark.read.parquet(path).cache()
  }

  def computeCounts(cols: Int, rows: Int, numFilters: Int, df: org.apache.spark.sql.DataFrame, spark: org.apache.spark.sql.SparkSession): org.apache.spark.sql.DataFrame = {
    spark.sqlContext.udf.register("eqUdf", (v1: Seq[Any], v2: Seq[String]) => if (v1.equals(v2)) 1 else 0)
    df.createOrReplaceTempView("df")

    val sql = createSql(cols, numFilters)

    spark.sql(sql)
  }

  def computeSums(numFilters: Int, dfCounts: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    import org.apache.spark.sql.functions.{col, sum}
    val dfSums = dfCounts.select(dfCounts.columns.map(c => sum(col(c))): _*)
      .toDF(Seq.range(0, numFilters).map(c => s"f${c}"): _*)
    dfSums
  }

}
