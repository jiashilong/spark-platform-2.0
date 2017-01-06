package com.jarry.spark.hive

import org.apache.spark.sql.SparkSession

/**
  * Created by jarry on 17/1/5.
  */
object HiveExampleApp {
    def main(args: Array[String]):Unit = {
        val spark = SparkSession.builder()
                                //必须是绝对路径
                                .config("spark.sql.warehouse.dir", "/Users/jarry/idea/app/spark-platform-2.0/warehouse")
                                .enableHiveSupport()
                                .getOrCreate()
        import spark.implicits._
        spark.sql("create table if not exists test(key int, value string)")
        spark.sql("load data local inpath 'src/main/resources/kv1.txt' into table test")
        spark.sql("select * from test").show()

        spark.close()
    }
}
