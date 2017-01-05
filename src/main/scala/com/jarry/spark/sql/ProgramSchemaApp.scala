package com.jarry.spark.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by jarry on 17/1/4.
  */
object ProgramSchemaApp {
    def main(args: Array[String]):Unit = {
        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._
        val df = spark.sparkContext.textFile("src/main/resources/people.txt")
        val ss = "name age"
        val fields = ss.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
        val schema = StructType(fields)
        val rs = df.map(_.split(",")).map(line => Row(line(0), line(1).trim()))
        val people = spark.createDataFrame(rs, schema)
        people.createTempView("v_people")
        val ts = spark.sql("select name from v_people")
        ts.map(r => "name: " + r(0)).collect().foreach(println)
        spark.close()
    }
}
