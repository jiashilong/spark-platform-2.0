package com.jarry.spark.hive

import org.apache.spark.sql.SparkSession

/**
  * Created by jarry on 17/1/10.
  */
object HiveSqlApp {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().enableHiveSupport().getOrCreate();
        val sql = args(0)

        import spark.implicits._
        spark.sql(sql).show()
        spark.close()
    }
}
