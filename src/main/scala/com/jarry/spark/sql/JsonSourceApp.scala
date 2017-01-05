package com.jarry.spark.sql

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by jarry on 17/1/4.
  */
object JsonSourceApp {
    def main(args: Array[String]):Unit = {
        val spark = SparkSession.builder().getOrCreate()
        val people = spark.read.format("json").load("src/main/resources/people.json")
        //val people = spark.read.json("src/main/resources/people.json")
        val ds = people.select("name", "age").cache()
        ds.write.mode(SaveMode.Overwrite).save("src/main/resources/people_name_age")
        ds.show()
        spark.close()
    }
}
