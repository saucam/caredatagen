package com.guavus.care.datagen

import java.io.File
import java.nio.file.{FileSystems, Path}
import java.util.Arrays
import org.apache.commons.io.FileUtils;

import org.apache.spark._
import org.apache.spark.sql.hive

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.mllib.random.RandomRDDs._

import scala.util.Random

object DataGen {
  def main(args : Array[String]) {
    val sparkConf = new SparkConf()
      .setAppName("CareDataGen")
      .setMaster("yarn-client")
      .set("spark.executor.memory", "24g")

    var input = new String;
    var output = new String;
    if (args.length < 2 ) {
      input = "/Users/yash.datta/Documents/Workspace/code/caredatagen/input/"

      output = "output"
    }
    else {
      input = args(0)
      output = args(1)
    }

    //FileUtils.deleteDirectory(new File(output))

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val hiveContext = new HiveContext(sc)

    val inputRdd = hiveContext.read.parquet(input).rdd

    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    inputRdd.cache()
    import hiveContext.implicits._

    //get total rows
    val inputSize = inputRdd.count

    val OSId = Array("Android", "iOS", "Windows")
    // select Os with distribution
    val dist = Array(List.fill(50)("Android"), List.fill(35)("iOS"), List.fill(15)("Windows")).flatMap(x=>x)
    // val dist = Map("Android" -> 0.5, "iOS" -> 0.35, "Windows" -> 0.15)


    //qoeRDD.cache

    //val cube1TempRdd = inputRdd.map(x => Cube1(x.getLong(0), Random.nextInt(5000), dist(Random.nextInt(100)), Random.nextInt(50).toLong, x.getLong(1),
    //                                       x.getLong(2), x.getLong(6), x.getLong(7), x.getLong(8), x.getLong(9), x.getLong(10), x.getLong(11)))

    val qoeRDD = poissonRDD(sc, 70.0, inputSize, inputRdd.partitions.size)

    val cube1Rdd = inputRdd.zipPartitions(qoeRDD, preservesPartitioning = true) { (thisIter, otherIter) =>
      new Iterator[Cube1] {
        def hasNext: Boolean = (thisIter.hasNext, otherIter.hasNext) match {
          case (true, true) => true
          case (false, false) => false
          case _ => throw new SparkException("Can only zip RDDs with " +
            "same number of elements in each partition")
        }
        def next() = {
          val x = thisIter.next()
          val cube1 = Cube1(x.getLong(0), Random.nextInt(5000).toLong, dist(Random.nextInt(100)), Random.nextInt(50).toLong, x.getLong(1),
            x.getLong(2), x.getLong(6), x.getLong(7), x.getLong(8), x.getLong(9), x.getLong(10), x.getLong(11), otherIter.next().toLong,
            x.getInt(12), x.getInt(13))

          (cube1)
        }
      }
    }.toDF()

    /* val cube1Rdd = cube1TempRdd.zipPartitions(qoeRDD, preservePartitioning = true, {(iter1, iter2) =>


    }) */
    cube1Rdd.write.partitionBy("exporttimestamp", "timestamp").mode(SaveMode.Append).format("parquet").saveAsTable(output)

    //read the written output


  }
}