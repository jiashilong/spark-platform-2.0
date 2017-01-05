package com.jarry.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by jarry on 17/1/4.
  */
object DataSetApp {
    case class Person(name:String, age:Int)

    def main(args: Array[String]):Unit = {
        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._

        val personDS = Seq(Person("Andy", 32)).toDS()  //隐式转换
        personDS.show()

        val primitiveDS = Seq(1,2,3).toDS();
        primitiveDS.map(_ + 1).collect().foreach(println);

        val peopleDS = spark.read.json("src/main/resources/people.json").as[Person]
        peopleDS.select("name", "age").show()

        spark.close()
    }
}
