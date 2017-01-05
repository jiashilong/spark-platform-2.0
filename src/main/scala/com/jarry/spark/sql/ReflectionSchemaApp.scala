package com.jarry.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by jarry on 17/1/4.
  */
object ReflectionSchemaApp {
    case class Person(name:String, age:Int)

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._

        val df = spark.read.textFile("src/main/resources/people.txt")
                      .map(_.split(","))
                      .map(t => Person(t(0), t(1).trim().toInt))
                      .toDF()
        df.createTempView("v_people")

        val teenagers = spark.sql("select name, age from v_people where age between 13 and 19")
        teenagers.map(t => "name: " + t(0)).show()
        teenagers.map(t => "name: " + t.getAs[String]("name") + ", age: " + t.getAs[Int]("age")).show()

        implicit val mm = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
        teenagers.map(t => t.getValuesMap[Any](List("name", "age"))).collect().foreach(println)

        spark.close()
    }
}
