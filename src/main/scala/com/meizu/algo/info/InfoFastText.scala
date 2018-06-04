package com.meizu.algo.info

import com.meizu.algo.classify.{ClassifyDataFrame, FastTextClassifier}
import com.meizu.algo.util._
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by dxp 2018-05-04
  */
object InfoFastText {

  val userPvTable = "algo.dxp_info_user_pv_day"
  val segTable = "algo.dxp_info_nouc_seg_day"
  val ucTable = "algo.dxp_info_uc_fid_cat_day"
  val parentCatTable = "algo.dxp_info_nouc_pcat_day"
  val childCatTable = "algo.dxp_info_nouc_ccat_day"
  val unionAllCatTable = "algo.dxp_info_all_cat_day"
  //val imeiCatTable = "algo.dxp_info_imei_cat_cnt_day"
  val imeiCatTable = "algo.dxp_info_imei_pcat_ccat_cnt_day"
  var isDebug = false

  // 一份代码中完成3个功能，根据参数来指定分类还是取关键词
  def main(args: Array[String]): Unit = {
    val op: String = args(0)
    isDebug = args(1).toLowerCase == "true"
    val statDate = args(2)
    op match {
      case "userpv" => queryUserPV(statDate)
      case "uc" => queryUcArticle(statDate)
      case "seg" => queryAndSegData(statDate)
      case "pcat" => {
        val cat = args(3)
        catParent(statDate, cat)
      }
      case "ccat" =>{
        val cat = args(3)
        catChild(statDate, cat)
      }
      case "union" => unionAllCatArticle(statDate)
      case "stat" => statCatCount(statDate)
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
  统计每天用户阅读的文章类别及总次数
   */
  def statCatCount(statDate: String) = {
    val sparkEnv = new SparkEnv("statCatCount").sparkEnv
    val ori_sql =
      s"""select a.imei,b.cat, a.sumcnt from (
         |select imei,fid,sumcnt from ${userPvTable} where stat_date=${statDate} ) a
         |join (
         |select fid,cat from ${unionAllCatTable} where stat_date=${statDate} ) b
         |on a.fid = b.fid
         |union all
         |select imei,category,pv from uxip.dwd_app_browser_click_article_log
         |where
         |stat_date=${statDate} and
         |category is not null and
         |category !=''
      """.stripMargin
    val data = sparkEnv.sql(ori_sql).rdd.map(r => {
      val cat: String = r.getAs[String](1).toLowerCase.replace("其它", "其他").replaceAll("[ _-]", ",")
      val (pcat, ccat) = if (cat.contains(",")) {
        val arr = cat.split(",")
        (arr(0), if (arr(1) == "other") arr(0) + "其他" else arr(1))
      } else {
        (cat, cat + "其他")
      }
      ((r.getAs[String](0), pcat, ccat), r.getAs[Long](2))
    }).reduceByKey(_ + _)

    val cols = Array("imei", "pcat", "ccat", "cnt")
    val colsType = Array("string", "string", "string", "long")
    val st = DXPUtils.createSchema(cols.zip(colsType))
    val df: DataFrame = sparkEnv.createDataFrame(data.map(r => {
      Row(r._1._1, r._1._2, r._1._3, r._2)
    }), st)
    DXPUtils.saveDataFrame(df, imeiCatTable, statDate, sparkEnv)
  }

  /*
  统计每天用户浏览文章及次数
   */
  def queryUserPV(statDate: String) = {
    val sparkEnv = new SparkEnv("queryUserPV").sparkEnv
    val ori_sql =
      s"""select imei,cast(article_id as bigint) as fid ,sum(read_cnt) as sumcnt from mzreader.dwd_app_stream_detail_reader
         |where stat_date=${statDate}
         |and imei is not null
         |and article_id is not null
         |and article_id !=''
         |and event_name='view_article' group by imei,article_id
      """.stripMargin
    val data = sparkEnv.sql(ori_sql)
    DXPUtils.saveDataFrame(data, userPvTable, statDate, sparkEnv)
  }

  /*
    找到每天uc文章
   */
  def queryUcArticle(statDate: String) = {
    val sparkEnv = new SparkEnv("seg_data").sparkEnv
    val ori_sql =
      s"""select a.fid,a.fcategory from mzreader.ods_t_article_c a
         | join (select distinct(fid) from ${userPvTable} where stat_date=${statDate}) b
         | on a.fid = b.fid where a.fcategory is not null and length(trim(a.fcategory)) > 1
      """.stripMargin
    val data = sparkEnv.sql(ori_sql)
    DXPUtils.saveDataFrame(data, ucTable, statDate, sparkEnv)
  }

  def unionAllCatArticle(statDate: String) = {
    val sparkEnv = new SparkEnv("unionAllCat").sparkEnv
    // 由于有的类别只有一级分类，不带二级分类, 需要从parentCatTable中将这些类别取出来.
    val onlyParentCat = Array("摄影","宠物","彩票","美文","干货","美食","幽默").map(r=>{
      "'" + r + "'"
    }).mkString(",")
    val ori_sql =
      s"""select fid,fcategory from ${ucTable} where stat_date=${statDate}
         | union all
         | select fid,cat from ${childCatTable} where stat_date=${statDate}
         | union all
         | select fid,cat from ${parentCatTable} where stat_date=${statDate}
         | and cat in (${onlyParentCat})
      """.stripMargin
    val data = sparkEnv.sql(ori_sql).toDF("fid", "cat")
    DXPUtils.saveDataFrame(data, unionAllCatTable, statDate, sparkEnv)
  }

  /*
  对每天用户浏览的非uc文章进行分词
   */
  def queryAndSegData(statDate: String) = {
    val sparkEnv = new SparkEnv("seg_data").sparkEnv
    /*
    val ori_sql =
      s"""select a.fid,a.ftitle,decode(unbase64(a.fcontent),'utf-8')  from mzreader.ods_t_article_c a
         | join (select distinct(fid) from ${userPvTable} where stat_date=${statDate} and fid is not null) b
         | on a.fid = b.fid where (a.fcontent is not null or a.ftitle is not null) and
         | (a.fcategory is null or length(trim(a.fcategory)) <= 1)
      """.stripMargin
      */

    val ori_sql =
      s"""select a.fid,a.ftitle,decode(unbase64(a.fcontent),'utf-8')  from mzreader.ods_t_article_c a
         | join (select distinct(fid) from ${userPvTable} where stat_date=${statDate} and fid is not null) b
         | on a.fid = b.fid where (a.fcontent is not null and a.fcontent != "") and
         | (a.fcategory is null or length(trim(a.fcategory)) <= 1)
      """.stripMargin

    val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql
    val data = sparkEnv.sql(sql).rdd.repartition(200).mapPartitions(it => {
      it.map(r => {
        val fid = r.getLong(0)
        val ftitle = r.getAs[String](1)
        val fcontent = r.getAs[String](2)

        val contentStr = if (fcontent == null) {
          ftitle
        } else {
          fcontent.trim.replaceAll("\n", "").replaceAll("<.*?>", "")
        }
        val content = Segment.segment2(contentStr)
        (fid, content)
      })
    })
    val cols = Array("fid", "words")
    val colsType = Array("long", "string")
    val st = DXPUtils.createSchema(cols.zip(colsType))
    val df: DataFrame = sparkEnv.createDataFrame(data.map(r => {
      Row(r._1, r._2.mkString(" "))
    }), st)
    DXPUtils.saveDataFrame(df, segTable, statDate, sparkEnv)
  }


  /*
  根据cat查询数据，结果存在分区表，分区有两个字段stat_date, cat
   */
  def catParent(statDate: String, cat: String) = {
    val sparkEnv = new SparkEnv(s"cat ${cat}").sparkEnv
    val ori_sql = s"select fid,words from ${segTable} where stat_date=${statDate} "
    val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql


    val catData = ClassifyDataFrame.classifyData(sparkEnv.sql(sql),cat)

    val cols = Array("fid", "words", "cat", "prob")
    val colsType = Array("long", "string", "string", "double")
    val st = DXPUtils.createSchema(cols.zip(colsType))
    val df: DataFrame = sparkEnv.createDataFrame(catData.map(r => {
      Row(r._1, r._2, r._3, r._4)
    }), st).repartition(200)
    DXPUtils.saveDataFrame(df, parentCatTable, statDate, sparkEnv)
  }

  def catChild(statDate: String, cat: String) = {
    val sparkEnv = new SparkEnv(s"cat ${cat}").sparkEnv
    val ori_sql = s"select fid,words from ${parentCatTable} where stat_date=${statDate} and cat='${cat}'"
    val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql

    val catData = ClassifyDataFrame.classifyData(sparkEnv.sql(sql),cat)

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


  /*
 对所有非uc文章进行分词
  */

  def queryAndSegAllData(statDate: String) = {
    val sparkEnv = new SparkEnv("seg_data").sparkEnv
    val ori_sql = "select fid,decode(unbase64(fcontent),'utf-8')  from mzreader.ods_t_article_c " +
      " where fid is not null and fcontent is not null and (fcategory is null or length(trim(fcategory)) < 1)"
    val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql
    val data = sparkEnv.sql(sql).rdd.repartition(1000).mapPartitions(it => {
      it.map(r => {
        val fid = r.getLong(0)
        val contentStr = r.getString(1).trim.replaceAll("\n", "").replaceAll("<.*?>", "")
        val content = Segment.segment2(contentStr)
        (fid, content)
      })
    }).filter(_._2.length > 10)
    val cols = Array("fid", "words")
    val colsType = Array("long", "string")
    val st = DXPUtils.createSchema(cols.zip(colsType))
    val df: DataFrame = sparkEnv.createDataFrame(data.map(r => {
      Row(r._1, r._2.mkString(" "))
    }), st)
    DXPUtils.saveDataFrame(df, segTable, statDate, sparkEnv)
  }

}

