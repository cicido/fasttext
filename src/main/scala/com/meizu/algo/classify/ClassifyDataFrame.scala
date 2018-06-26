package com.meizu.algo.classify

import com.meizu.algo.util.Log
import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object ClassifyDataFrame {

  def loadSingleModel(cat: String) = {
    val catFile = s"${cat}.mode.ftz"
    val tmpFile = SparkFiles.get(catFile)
    val m = new FastTextClassifier(tmpFile, cat)
    (cat, m)
  }

  /*
  对DataFrame记录进行预测,DataFrame只含有两个字段(id:Long, words: String)
  cat表示要加载的模型
 */
  def classifyData(df: DataFrame, cat: String): RDD[(Long, String, String, Double)] = {

    df.rdd.mapPartitions(e => {
      val (key, parentModel) = loadSingleModel(cat)
      e.map(r => {
        val pcat = {
          try {
            parentModel.predictWithProb(r.getAs[String](1))
          } catch {
            case ex: Throwable => {
              Log.info("error :" + ex.getMessage)
              Log.info(s"** ${r.getAs[Long](0)}")
              ("unknow", -1.0)
            }
          }
        }
        (r.getAs[Long](0), r.getAs[String](1), pcat._1, pcat._2)
      })
    })

  }


}
