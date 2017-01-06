package com.jarry.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by jarry on 17/1/6.
  */
object JdbcApp {
    def main(args: Array[String]):Unit = {
        val spark = SparkSession.builder().getOrCreate()
        val users = spark.read.format("jdbc")
                            .option("driver", "com.mysql.jdbc.Driver")
                            .option("url", "jdbc:mysql://localhost:3306/canal")
                            .option("dbtable", "user")
                            .option("user", "root")
                            .option("password", "root")
                            .load()
        users.show()

        spark.close()
    }
}
