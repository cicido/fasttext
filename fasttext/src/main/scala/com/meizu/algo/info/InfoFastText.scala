package com.meizu.algo.info

import com.meizu.algo.classify.ClassifyDataFrame
import com.meizu.algo.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by dxp 2018-05-04
  */
object InfoFastText {

  val userPvTable = "algo.dxp_info_user_pv_day"
  val ucTable = "algo.dxp_info_uc_fid_cat_day"

  val titleSegTable = "algo.dxp_info_nouc_title_seg_day"
  val contentSegTable = "algo.dxp_info_nouc_content_seg_day"

  val titlePcatTable = "algo.dxp_info_nouc_title_pcat_day"
  val contentPcatTable = "alog.dxp_info_nouc_content_pcat_day"
  val titleCcatTable = "algo.dxp_info_nouc_title_ccat_day"
  val contentCcatTable = "algo.dxp_info_nouc_content_ccat_day"

  val unionAllCatTable = "algo.dxp_info_fid_cat_day"

  val imeiCatTable = "algo.dxp_info_imei_pcat_ccat_cnt_day"
  var isDebug = false

  val subCats = Array("两性情感","体育","健康","军事","历史","国际",
    "娱乐","房产","教育","旅游","时尚","星座",
    "汽车","游戏","社会","科学探索","科技","育儿","财经")

  // 一份代码中完成3个功能，根据参数来指定分类还是取关键词
  def main(args: Array[String]): Unit = {
    val op: String = args(0)
    isDebug = args(1).toLowerCase == "true"
    val statDate = args(2)
    op match {
      case "userpv" => queryUserPV(statDate)
      case "uc" => queryUcArticle(statDate)
      case "seg" => queryAndSegData(statDate,titleSegTable, contentSegTable)
      case "title" => {
        catParent(statDate,titleSegTable, titlePcatTable)
        catChild(statDate,titlePcatTable,titleCcatTable)
      }
      case "content" =>{
        catParent(statDate,contentSegTable, contentPcatTable)
        catChild(statDate,contentPcatTable, contentCcatTable)
      }
      case "union" => {
        val uType = args(3).toInt
        unionAllCatArticle(statDate,uType)
      }
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

  def unionAllCatArticle(statDate: String, uType:Int) = {
    val sparkEnv = new SparkEnv("unionAllCat").sparkEnv
    // 由于有的类别只有一级分类，不带二级分类, 需要从parentCatTable中将这些类别取出来.
    val onlyParentCat = Array("摄影","宠物","彩票","美文","干货","美食","幽默").map(r=>{
      "'" + r + "'"
    }).mkString(",")
    /*
    utype=1, 表示只取uc类别数据
    utype=2, 表示取uc类别数据与非uc中文章不为空的数据
    utype=其他，表示合并uc类别数据及非uc中标题或内容算出来的类别
     */
    val ucSql = "select fid,fcategory from ${ucTable} where stat_date=${statDate}"
    val noUcSql =
      s"""select fid,cat,prob from ${titlePcatTable} where stat_date=${statDate}
         | union all
         | select fid,cat,prob from ${titleCcatTable} where stat_date=${statDate}
         | union all
         | select fid,cat,prob from ${contentPcatTable} where stat_date=${statDate}
         | union all
         | select fid,cat,prob from ${contentCcatTable} where stat_date=${statDate}
      """.stripMargin
    if(uType == 1){
      val data = sparkEnv.sql(ucSql).toDF("fid", "cat")
      DXPUtils.saveDataFrame(data, unionAllCatTable, statDate, sparkEnv)
    }else{
      /*
      分情况统计：
      1. 父类是unknow,去掉
      2. 根据union后的catArray长度分析，在判断之前，先按cat长度从小到大排序.
      i. 是否存在包含关系.
      ii. 如果不存在包含关系,则按概率进行选择.
       */
      val ucData:RDD[(Long,String)] = sparkEnv.sql(ucSql).rdd.map(r=>(r.getAs[Long](0),r.getAs[String](1)))

      def selectCat(x:(String,Double),y:(String,Double)): (String,Double) = {
        if(y._1.contains(x._1) || y._2 >= x._2 ) y else x
      }

      val noUcData = sparkEnv.sql(noUcSql).rdd.map(r=>{
        (r.getAs[Long](0),r.getAs[String](1),r.getAs[Double](2))
      }).filter(_._2 != "unknow").map(r=>(r._1,Array((r._2,r._3)))).reduceByKey(_ ++ _).map(r=>{
        val fid = r._1
        val (cat,prob) = r._2.sortWith(_._1.length < _._1.length).reduceLeft(selectCat)  // 按字符串长度排序.
        (fid,cat)
      })
      val cols = Array("fid", "cat")
      val colsType = Array("long", "string")
      val st = DXPUtils.createSchema(cols.zip(colsType))
      val df: DataFrame = sparkEnv.createDataFrame(ucData.union(noUcData).map(r => {
        Row(r._1, r._2)
      }), st).repartition(200)
      DXPUtils.saveDataFrame(df, unionAllCatTable, statDate, sparkEnv)
    }
  }

  /*
  对每天用户浏览的非uc文章进行分词
   */
  def queryAndSegData(statDate: String, titleSegTable:String, contentSegTable:String) = {
    val sparkEnv = new SparkEnv("seg_data").sparkEnv

    val ori_sql =
      s"""select a.fid,a.ftitle,decode(unbase64(a.fcontent),'utf-8')  from (select distinct(fid) as fid from ${userPvTable}
         | where stat_date=${statDate} and fid is not null) b
         | join mzreader.ods_t_article_c a
         | on a.fid = b.fid where a.fcategory is null or length(trim(a.fcategory)) <= 1
      """.stripMargin

    val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql
    val data = sparkEnv.sql(sql).rdd.repartition(200).mapPartitions(it => {
      it.map(r => {
        val fid = r.getLong(0)
        val ftitle = r.getAs[String](1)
        val fcontent = r.getAs[String](2)
        val titleStr = if (ftitle == null){
          ""
        } else {
          ftitle.trim.replaceAll("\n"," ").replaceAll("<.*?>", " ")
        }
        val title = Segment.segment2(titleStr)

        val contentStr = if (fcontent == null) {
          ""
        } else {
          fcontent.trim.replaceAll("\n", " ").replaceAll("<.*?>", " ")
        }
        val content = Segment.segment2(contentStr)
        (fid, title, content)
      })
    })
    data.cache()

    val cols = Array("fid", "words")
    val colsType = Array("long", "string")
    val st = DXPUtils.createSchema(cols.zip(colsType))
    val titleDF: DataFrame = sparkEnv.createDataFrame(data.map(r => {
      Row(r._1, r._2.mkString(" "))
    }), st)
    DXPUtils.saveDataFrame(titleDF, titleSegTable, statDate, sparkEnv)

    val contentDF: DataFrame = sparkEnv.createDataFrame(data.map(r => {
      Row(r._1, r._3.mkString(" "))
    }), st)
    DXPUtils.saveDataFrame(contentDF, titleSegTable, statDate, sparkEnv)
  }


  /*
  根据cat查询数据，结果存在分区表，分区有两个字段stat_date, cat
   */
  def catParent(statDate: String, inputTable:String, outputTable:String) = {
    val cat = "all"
    val sparkEnv = new SparkEnv(s"cat ${cat}").sparkEnv
    val ori_sql = s"select fid,words from ${inputTable} where stat_date=${statDate} "
    val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql

    val catData = ClassifyDataFrame.classifyData(sparkEnv.sql(sql),cat)

    val cols = Array("fid", "words", "cat", "prob")
    val colsType = Array("long", "string", "string", "double")
    val st = DXPUtils.createSchema(cols.zip(colsType))
    val df: DataFrame = sparkEnv.createDataFrame(catData.map(r => {
      Row(r._1, r._2, r._3, r._4)
    }), st).repartition(200)
    DXPUtils.saveDataFrame(df, outputTable, statDate, sparkEnv)
  }

  def catChild(statDate: String, inputTable:String, outputTable:String) = {
    val sparkEnv = new SparkEnv(s"cat child").sparkEnv

    val cat = subCats(0)
    val ori_sql = s"select fid,words from ${inputTable} where stat_date=${statDate} and cat='${cat}'"
    val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql
    var catData:RDD[(Long,String,String,Double)] = ClassifyDataFrame.classifyData(sparkEnv.sql(sql),cat)

    for(cat <- subCats.slice(1,subCats.length)){
      val ori_sql = s"select fid,words from ${inputTable} where stat_date=${statDate} and cat='${cat}'"
      val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql
      catData = catData.union(ClassifyDataFrame.classifyData(sparkEnv.sql(sql),cat))
    }

    /*
    val rddArray:Array[RDD[(Long,String,String,Double)]] = subCats.map(cat=>{
      val ori_sql = s"select fid,words from ${parentCatTable} where stat_date=${statDate} and cat='${cat}'"
      val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql
      ClassifyDataFrame.classifyData(sparkEnv.sql(sql),cat)
    })

    val myunion = (x:RDD[(Long,String,String,Double)], y:RDD[(Long,String,String,Double)]) => { x.union(y)}

    val catData = rddArray.reduce(myunion)
    */


    val cols = Array("fid", "words", "cat", "prob")
    val colsType = Array("long", "string", "string", "double")
    val st = DXPUtils.createSchema(cols.zip(colsType))
    val df: DataFrame = sparkEnv.createDataFrame(catData.map(r => {
      Row(r._1, r._2, r._3, r._4)
    }), st).repartition(200)

    DXPUtils.saveDataFrame(df, outputTable, statDate, sparkEnv)
  }

  /*
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
  */


}

