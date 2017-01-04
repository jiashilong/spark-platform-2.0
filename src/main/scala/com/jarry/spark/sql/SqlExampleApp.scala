package com.jarry.spark.sql

import org.apache.spark.sql.SparkSession


/**
  * Created by jarry on 17/1/3.
  */
object SqlExampleApp {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().getOrCreate();
        import spark.implicits._

        val df = spark.read.json("src/main/resources/people.json")
        df.show()
        df.printSchema()
        df.select($"name").show()
        df.select($"name", $"age"+1).show()   //这里需要使用隐式转换, import spark.implicits._
        df.filter($"age" > 21).show()
        df.groupBy($"age").count().show()
        spark.stop()
    }
}
