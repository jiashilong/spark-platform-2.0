package com.jarry.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @param name:  姓名
  * @param score: 积分
  * 规则：姓名升序，积分降序。
  */
class SortedKey(val name:String, val score:Int) extends Ordered[SortedKey] with Serializable {
    def compare(that: SortedKey): Int = {
        val comp = this.name.compareTo(that.name)
        if(comp == 0) {
            return this.score.compareTo(that.score) * (-1) //降序积分
        } else {
            return comp
        }
    }

    override def toString: String = "[" + this.name + "," + this.score + "]"
}

/**
  * Created by jarry on 17/1/3.
  */
object SecondarySortApp {
    def main(args: Array[String]):Unit = {
        val conf = new SparkConf()
        val ctx = new SparkContext(conf)
        val lines = ctx.textFile("src/main/resources/data.txt")
        val pairs = lines.map (line=> (new SortedKey(line.split("\\s")(0) ,line.split("\\s")(1).toInt), 1))
                         .sortByKey(true)

        pairs.collect().foreach(println)
        ctx.stop()
    }
}
