package com.meizu.algo.traindata

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}
import java.util

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import com.meizu.algo.util.{DXPUtils, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

import scala.collection.mutable

/*
抽取资讯中fcategory不为空且长度大于1的文章，对类别进行转换，对标题,内容进行分词
 */
object InfoTrainData {
  // 分词，计算hash
  val trainTable = s"algo.dxp_info_v1_train_data"

  var isDebug:Boolean  =  false

  def main(args: Array[String]): Unit = {
    val op: String = args(0)
    isDebug = args(1).toLowerCase == "true"
    val statDate = args(2)
    op match {
      case "data" => {
        val sparkEnv = new SparkEnv("data").sparkEnv
        // 没有限定fresource_type=2, 可能导致视频被加进来,而视频的内容为空
        val ori_sql = "select fcategory cat,ftitle title,decode(unbase64(fcontent),'utf-8') content, fresource_type from " +
          " mzreader.ods_t_article_c where fcategory is not null and length(trim(fcategory)) > 1 "
        val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql

        val data = sparkEnv.sql(sql)
        println("*"*100)
        println("data partitions:" + data.rdd.getNumPartitions)
        val repartData = if (data.rdd.getNumPartitions < 200) data.repartition(200) else data

        val trainData = makeTrainData(repartData)
        val cols = Array("cat","title","content","resource_type")
        val colsType = Array("string","string","string","long")
        val st = DXPUtils.createSchema(cols.zip(colsType))
        val df: DataFrame = sparkEnv.createDataFrame(trainData.map(r => {
          Row(r._1, r._2,r._3,r._4)
        }), st).repartition(200)
        DXPUtils.saveDataFrame(df,trainTable,statDate,sparkEnv)
      }
      case "save" =>{
        val sparkEnv = new SparkEnv("save").sparkEnv

        // 建立本地文件目录,以保存训练文件
        val outDirPrefix = args(3) // /hadoop/data08/linjiang/db/fastsub.$(date +%y%m%d-%H)
        val outDir = new File(outDirPrefix + "/fastsub." + statDate)
        if (!outDir.exists()) outDir.mkdir()

        // 建立写文件handle
        val hasSubCat = CatTransform.usedCat.groupBy(_.split(",")(0)).filter(_._2.size > 2).keySet + "all"
        println("hasSubcat:")
        hasSubCat.foreach(r => println(r))
        val writers = makeWriter(outDir, hasSubCat)

        // 类别计数
        val wrtCntMap = new mutable.HashMap[String, Int]().withDefaultValue(0)

        /* 这里查询时可选择是title还是content
        val ori_sql = s"select cat, content from algo.dxp_info_v1_train_data where stat_date=${statDate} " +
          s" and resource_type = 2 and cat is not null and length(trim(cat))>1 and content is not null and length(trim(content)) > 1"
        */
        val ori_sql = s"select cat, title from algo.dxp_info_v1_train_data where stat_date=${statDate} " +
          s" and resource_type = 2 and cat is not null and length(trim(cat))>1 and " +
          s" title is not null and length(trim(title)) > 1"

        val sql = if (isDebug) ori_sql + " limit 100000" else ori_sql

        val data = sparkEnv.sql(sql)
        println("*"*100)
        println("data partitions:" + data.rdd.getNumPartitions)
        val repartData = if (data.rdd.getNumPartitions < 200) data.repartition(200) else data

        //对未在指定类别集的类别转化为unknow

        var totalCnt = 0
        val label= CatTransform.labelPrefix

        val rddData = repartData.rdd.map(r=>{
          val cat = r.getAs[String](0)
          val transCat =
            if (CatTransform.usedCat.contains(cat)) {
              cat.replace(",", "_")
            } else if (cat.contains(",")) {
              val pcat = cat.trim.split(",")(0)
              val ccat = cat.trim.split(",")(1)
              if (CatTransform.usedCat.contains(pcat)) pcat
              else if(CatTransform.usedCat.contains(ccat)) ccat
              else "unknow"
            } else {
              "unknow"
            }
          (transCat, r.getAs[String](1))
        }).toLocalIterator.foreach(r => {
          val cat = r._1
          val content = r._2

          wrtCntMap(cat) += 1

          val i = cat.lastIndexOf("_")
          val outCat = if (i > 0) cat.substring(0, i) else cat
          //如果增加i>0的判断,会导致缺少一个子类，比如只标注了"体育"的文章,将不会被训练到.
          if (i>0 && hasSubCat.contains(outCat)) {
            val out = writers(outCat)
            out.write(label + cat)
            out.write("\t")
            out.write(content)
            out.write("\n")
          }
          val out = writers("all")
          out.write(label + outCat)
          out.write("\t")
          out.write(content)
          out.write("\n")

          totalCnt += 1
          if (totalCnt % 10000 == 0) {
            println(s"fihish cnt=$totalCnt")
          }
        })
        finishWriter(writers)


        val catOut = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(outDir, "cat"))))
        catOut.write("train=" + wrtCntMap.map(e => e._1 + ":" + e._2).mkString(",") + "\n")
        catOut.close()

        println("finish .......")
      }
    }
  }

  def makeWriter(outFile: File, name: Set[String]): Map[String, BufferedWriter] = {

    name.map(n => {
      (n, new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(outFile, "dat." + n)))))
    }).toMap

  }

  def finishWriter(writers: Map[String, BufferedWriter]): Unit = {
    writers.foreach(_._2.close())
  }

  /*
  cat, title, content,resource_type
   */
  def makeTrainData(df:DataFrame): RDD[(String,String,String,Long)] = {
    import scala.collection.JavaConversions._
    // 去掉标点与字符串(字串符可能使词典规模大增, 字词模式或者NGrams，可以考虑加进来)
    df.rdd.map(r=>{
      val cat = CatTransform.transform(r.getAs[String](0))
      //val cat = r.getAs[String](0)
      //去掉词性cdempqruwxyz
      val title = if(r.getAs[String](1)==null) "" else
        seg(stripHtml(r.getAs[String](1))).filterNot(t=>"cdempqruwxyz".contains(t.nature.firstChar()) || t.word.length < 2).map(_.word).mkString(" ")
      val content = if(r.getAs[String](2)==null) "" else
        seg(stripHtml(r.getAs[String](2))).filterNot(t=>"cdempqruwxyz".contains(t.nature.firstChar()) || t.word.length < 2).map(_.word).mkString(" ")
      (cat,title,content,r.getAs[Long](3))
    })
  }

  def stripHtml(content: String): String = {
    if (content== null || content.isEmpty) ""
    else {
      content.replaceAll("\n"," ").replaceAll("<script>.*?</script>","")
        .replaceAll("(</p>|</br>)\n*", "\n")
        .replaceAll("<[^>]+?>" , " ")
        .replaceAll("(点击加载图片)|(查看原文)|(图片来源)|([\\-#=]{3,})", " ")
        .replaceAll("\\s*\n\\s*", "\n")
        .replaceAll("[ \t]+", " ")
    }
  }

  def seg(sen: String):util.List[Term] = {
    Seg.segment(sen)
  }

  object Seg {
    val SEG = HanLP.newSegment()
    SEG.enableNumberQuantifierRecognize(true)
    SEG.enableCustomDictionary(true)
    //若开启人名识别,会导致出现大量的人名，地名，机构名. 不如直接用百科词典匹配
    SEG.enableNameRecognize(false)

    def segment(text: String): util.List[Term] = {
      try {
        SEG.seg(HanLP.convertToSimplifiedChinese(text))
      } catch {
        case e: Exception => util.Collections.emptyList()
      }
    }
  }

}
