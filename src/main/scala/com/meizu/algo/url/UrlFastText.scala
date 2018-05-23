package com.meizu.algo.url

import java.io._

import com.meizu.algo.classify.FastTextClassifier
import com.meizu.algo.util._
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

/**
  * Created by dxp 2018-05-04
  */
object UrlFastText {
  val business = "url"
  val version = "v1"
  val segTable = s"algo.dxp_${business}_${version}_simhash_seg"
  val keyTable = s"algo.dxp_${business}_${version}_fasttext_key"
  val parentCatTable = s"algo.dxp_${business}_${version}_fasttext_pcat"
  val childCatTable = s"algo.dxp_${business}_${version}_fasttext_ccat"
  val dupTable = s"algo.dxp_${business}_${version}_fasttext_dup"
  var isDebug = false

  /*
  分别进行分词，父分类，子分类
   */
  def main(args: Array[String]): Unit = {
    val op: String = args(0)
    isDebug = args(1).toLowerCase == "true"
    val statDate = args(2)
    op match {
      case "key" => keyExtract(statDate)
      case "pcat" => {
        val cat = args(3)
        catParent(statDate, cat)
      }
      case "ccat" => {
        val cat = args(3)
        catChild(statDate, cat)
      }
      case "dup" => {
        val thres = args(3).toInt
        saveDup(statDate,thres)
      }
      case _ => {
        println(
          """
            |Usage: [cat|catprob|catdis]
          """.stripMargin)
        sys.exit(1)
      }
    }
  }

  /*
  使用之前simhash已分好的词表, 因已只作一些筛选
  对
   */
  def keyExtract(statDate: String) = {
    val sparkEnv = new SparkEnv("key").sparkEnv

    val ori_sql = "select fid,words " +
      s"from ${segTable} where stat_date=${statDate}"


    val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql
    val data = sparkEnv.sql(sql).rdd.filter(r => {
      val words = r.getAs[String](1)
      words != null && words.trim.length > 1
    }).map(r => {
      val words = r.getAs[String](1).split(",").map(w => {
        val arr = w.split("/")
        (arr(0), arr(1))
      }).filterNot(w => {
        val nt = w._2
        // 去掉以cdempqruwxyz开头的词性
        "w".contains(nt.substring(0, 1))
      }).map(_._1)
      (r.getAs[Long](0), words.mkString(" "))
    })

    val cols = Array("fid", "words")
    val colsType = Array("long", "string")
    val st = DXPUtils.createSchema(cols.zip(colsType))
    val df: DataFrame = sparkEnv.createDataFrame(data.map(r => {
      Row(r._1, r._2)
    }), st)
    DXPUtils.saveDataFrame(df, keyTable, statDate, sparkEnv)
  }


  /*
  根据cat查询数据，结果存在分区表，分区有两个字段stat_date, cat
   */
  def catParent(statDate: String, cat: String) = {
    val sparkEnv = new SparkEnv(s"cat ${cat}").sparkEnv
    val ori_sql = s"select fid,words from ${keyTable} where stat_date=${statDate} "
    val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql


    val catData = sparkEnv.sql(sql).rdd.mapPartitions(e => {
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

    val cols = Array("fid", "words", "cat", "prob")
    val colsType = Array("long", "string", "string", "double")
    val st = DXPUtils.createSchema(cols.zip(colsType))
    val df: DataFrame = sparkEnv.createDataFrame(catData.map(r => {
      Row(r._1, r._2, r._3, r._4)
    }), st)
    DXPUtils.saveDataFrame(df, parentCatTable, statDate, sparkEnv)
  }

  def catChild(statDate: String, cat: String) = {
    val sparkEnv = new SparkEnv(s"cat ${cat}").sparkEnv
    val ori_sql = s"select fid,words from ${parentCatTable} where stat_date=${statDate} and cat='${cat}'"
    val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql

    val catData = sparkEnv.sql(sql).rdd.mapPartitions(e => {
      val (key, childModel) = loadSingleModel(cat)
      e.map(r => {
        val pcat = {
          try {
            childModel.predictWithProb(r.getAs[String](1))
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

    val cols = Array("fid", "words", "cat", "prob")
    val colsType = Array("long", "string", "string", "double")
    val st = DXPUtils.createSchema(cols.zip(colsType))
    val df: DataFrame = sparkEnv.createDataFrame(catData.map(r => {
      Row(r._1, r._2, r._3, r._4)
    }), st)
    // 修改成多分区
    val catPinYin = StrUtil.getPinyinString(cat)
    DXPUtils.saveDataFrameWithTwoPartition(df, childCatTable, statDate, catPinYin, sparkEnv)
  }

  // 找出重复量大于thres的数据,且找到fid对应的category, 以便业务核查
  def saveDup(statDate:String, thres:Int) = {
    val sparkEnv = new SparkEnv("kbits").sparkEnv
    //val inputTable = s"${distanceTable}_1_3"
    val outputTable = s"algo.dxp_simhash_threshold_${thres}"
    val ori_sql: String = "select a.fid, a.hash, a.samelen, b.furl,b.ftitle,b.fkeywords,b.fcontent,c.cat,c.prob from " +
      s" (select fid, hash, samelen from algo.dxp_url_simhash_distance_1_3 where stat_date=${statDate} " +
      s" and samelen >= ${thres}) a join (select furl,fid,ftitle,fkeywords,fcontent from " +
      s" uxip.dwd_browser_url_creeper where stat_date=${statDate}) b " +
      s" on a.fid = b.fid " +
      s" join (select fid,cat,prob from ${childCatTable} where stat_date=${statDate}) c " +
      s" on b.fid = c.fid "

    // 采用udf函数对furl进行处理
    val stripUrl = udf[String,String]{ w =>
      try{
        if(w == null) "" else w.split("/").take(3).mkString("/")
      }catch{
        case _ => ""
      }
    }

    val replaceStr = udf[String,String]{w=>{
      try{
        if (w == null) "" else w.replaceAll("[\n\r\t ,]+","")
      }catch{
        case _ => ""
      }

    }}
    val sql: String = if (isDebug) ori_sql + " limit 10000" else ori_sql
    val data = sparkEnv.sql(sql).toDF("fid","hash","samelen","furl", "ftitle","fkeywords","fcontent","cat","prob")
    DXPUtils.saveDataFrame(data.
      withColumn("furl",stripUrl(data("furl"))).
      withColumn("ftitle",replaceStr(data("ftitle"))).
      withColumn("fkeywords",replaceStr(data("fkeywords"))).
      withColumn("fcontent",replaceStr(data("fcontent"))),dupTable,statDate,sparkEnv)
  }

  def loadSingleModel(cat: String) = {
    val catFile = s"${cat}.mode.ftz"
    val tmpFile = SparkFiles.get(catFile)
    val m = new FastTextClassifier(tmpFile, cat)
    (cat, m)
  }

}

