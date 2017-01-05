package com.jarry.spark.sql

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by jarry on 17/1/5.
  */
object ParquetSchemaMergeApp {
    def main(args: Array[String]):Unit = {
        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._

        val squares = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i*i)).toDF("value", "square")
        squares.write.mode(SaveMode.Overwrite).parquet("/tmp/data/scheme_merge/key=square")
        squares.show()

        val cubes = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i*i*i)).toDF("value", "cube")
        cubes.write.mode(SaveMode.Overwrite).parquet("/tmp/data/scheme_merge/key=cube")
        cubes.show()

        val merges = spark.read//.option("mergeSchema", "true")
                          .parquet("/tmp/data/scheme_merge")
        merges.printSchema()
        merges.show()

        spark.close()

    }
}
