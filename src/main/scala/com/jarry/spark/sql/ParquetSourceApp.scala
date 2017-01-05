package com.jarry.spark.sql

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by jarry on 17/1/4.
  */
object ParquetSourceApp {
    def main(args: Array[String]):Unit = {
        val spark = SparkSession.builder().getOrCreate()
        val users = spark.read.load("src/main/resources/users.parquet")
        users.show()
        //覆盖已存在的数据
        users.select("name", "favorite_color").write.mode(SaveMode.Overwrite).save("src/main/resources/user_favorite_color")

        spark.read.load("src/main/resources/user_favorite_color").show()
        spark.close()
    }
}
