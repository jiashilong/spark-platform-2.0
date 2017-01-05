package com.jarry.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by jarry on 17/1/5.
  */
object JsonDatasetApp {
    def main(args: Array[String]):Unit = {
        val spark = SparkSession.builder().getOrCreate()
        val df1 = spark.read.json("src/main/resources/people.json")
        df1.createOrReplaceTempView("v_people")
        spark.sql("select * from v_people").collect().foreach(println)

        val r = spark.sparkContext.makeRDD("""{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
        val df2 = spark.read.json(r)
        df2.show()

        spark.close()
    }
}
