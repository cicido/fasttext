package com.meizu.algo.traindata

import java.io._

import com.meizu.algo.util.{DXPUtils, SparkEnv}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame

object SaveHiveDataToLocal {
  val fidFile = "hdfs:///tmp/duanxiping/fasttext/data/discard_fid.txt"
  var isDebug = false

  def main(args: Array[String]): Unit = {
    val op: String = args(0)
    isDebug = args(1).toLowerCase == "true"
    val statDate = args(2)
    val localPath = args(3)
    val sparkEnv = new SparkEnv("data export").sparkEnv

    val writer = new PrintWriter(new File(localPath))


    val discardFidArr = sparkEnv.sparkContext.textFile(fidFile).filter(r => {
      r != null && r.trim.length > 0
    }).map(_.trim.toLong).collect()
    discardFidArr.foreach(r => println(r))

    val discardHashArrBr: Broadcast[Array[Long]] = sparkEnv.sparkContext.broadcast(discardFidArr)
    val ori_sql = s"select fid, words,hash from  algo.dxp_url_v1_fasttext_seg_hash where stat_date=${statDate}"
    val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql

    sparkEnv.sql(sql).rdd.
      map(r => (r.getAs[Long](0), r.getAs[String](1), r.getAs[Long](2))).
      filter(r => {
        discardFidArr.contains(r._1)
      }).collect().foreach(r=>{
      writer.write(Array(r._1.toString,r._2,r._3.toString).mkString(",") + "\n")
    })
    writer.close()
  }

}
