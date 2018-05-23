package com.meizu.algo.url

import com.meizu.algo.classify.FastTextClassifier
import com.meizu.algo.util._
import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by dxp 2018-05-04
  */
object UrlFastTextLess {
  val business = "url"

  //设定version, 以对比算法改进效果.
  //当分词，hash方法，fasttext模型等发生变化，能够通过version字段
  //生成不同的表，以实现效果对比
  val version = "v1"
  // 分词，计算hash
  val keyTable = s"algo.dxp_${business}_${version}_fasttext_seg_hash"
  val discardHashFile = "hdfs:///tmp/duanxiping/fasttext/shell/discard_hash.txt"

  // fasttext分类
  val parentCatTable = s"algo.dxp_${business}_${version}_fasttext_pcat"
  val childCatTable = s"algo.dxp_${business}_${version}_fasttext_ccat"
  val dupTable = s"algo.dxp_${business}_${version}_fasttext_dup"

  // simhash计算
  val sameTable = s"algo.dxp_${business}_${version}_simhash_same"
  val distanceTable = s"algo.dxp_${business}_${version}_simhash_distance"

  var isDebug = false

  /*
  分别进行分词，父分类，子分类
   */
  def main(args: Array[String]): Unit = {
    val op: String = args(0)
    isDebug = args(1).toLowerCase == "true"
    val statDate = args(2)
    op match {
      case "key" => {
        // 将sql数据导出与写入从函数中抽离.
        val sparkEnv = new SparkEnv("key").sparkEnv
        val ori_sql = "select fid,fcontent " +
          s"from uxip.dwd_browser_url_creeper where stat_date=${statDate}"
        val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql

        val discardHashArr = sparkEnv.read.textFile(discardHashFile).rdd.map(_.toLong).collect()


        val data:RDD[(Long,String,Long)] = keyExtract(sparkEnv.sql(sql).repartition(200),discardHashArr)

        val cols = Array("fid", "words","hash")
        val colsType = Array("long", "string","long")
        val st = DXPUtils.createSchema(cols.zip(colsType))
        val df: DataFrame = sparkEnv.createDataFrame(data.map(r => {
          Row(r._1, r._2,r._3)
        }), st)
        DXPUtils.saveDataFrame(df, keyTable, statDate, sparkEnv)
      }
      case "pcat" => {
        val cat = args(3)
        val sparkEnv = new SparkEnv(s"cat ${cat}").sparkEnv
        val ori_sql = s"select fid,words from ${keyTable} where stat_date=${statDate} "
        val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql

        val catData = classifyData(sparkEnv.sql(sql), cat)

        val cols = Array("fid", "words", "cat", "prob")
        val colsType = Array("long", "string", "string", "double")
        val st = DXPUtils.createSchema(cols.zip(colsType))
        val df: DataFrame = sparkEnv.createDataFrame(catData.map(r => {
          Row(r._1, r._2, r._3, r._4)
        }), st)
        DXPUtils.saveDataFrame(df, parentCatTable, statDate, sparkEnv)
      }
      case "ccat" => {
        val cat = args(3)
        val sparkEnv = new SparkEnv(s"cat ${cat}").sparkEnv
        val ori_sql = s"select fid,words from ${parentCatTable} where stat_date=${statDate} and cat='${cat}'"
        val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql

        val catData = classifyData(sparkEnv.sql(sql), cat)

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
      case "same" => findSame(statDate)
      case "kbits"=>{
        val kBits = args(3).toInt
        val bId = args(4).toInt
        findkBitsDistance(statDate, kBits, bId)
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
   */
  def keyExtract(df:DataFrame,discardHashArr:Array[Long]):RDD[(Long,String,Long)] = {

    df.rdd.filter(r => {
      val words = r.getAs[String](1)
      words != null && words.trim.length > 1
    }).map(r => {
      val words = Segment.segmentWithNature(r.getAs[String](1).replaceAll("[^\u4e00-\u9FCB]+", " ")).map(w => {
        (w.word,w.nature.toString)
      }).filterNot(w => {
        val nt = w._2
        // 去掉以cdempqruwxyz开头的词性
        "w".contains(nt.substring(0, 1))
      })

      // 去掉某些词性的词后，词序列长度至少大于5才进行hash运算.
      val hashWords = words.filterNot(w=>"cdempqruwxyz".contains(w._2.substring(0,1)))
      val hash:Option[Long] = if(hashWords.length < 5) None else Some(FNVHash.hashContent(words.
        map(_._1)).longValue())

      (r.getAs[Long](0), words.map(_._1).mkString(" "),hash)
    }).filter(r=>{
      !r._3.isEmpty && {
        var flag: Boolean = false
        for (dHash <- discardHashArr if !flag) {
          flag = FNVHash.distance(r._3.get, dHash, 1)
        }
        !flag
      }
    }).map(r=>(r._1,r._2,r._3.get))
  }


  /*
  根据cat查询数据，结果存在分区表，分区有两个字段stat_date, cat
   */
  def classifyData(df:DataFrame, cat: String):RDD[(Long,String,String,Double)] = {

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


  // 找出重复量大于thres的数据,且找到fid对应的category, 以便业务核查
  def saveDup(statDate:String, thres:Int) = {
    val sparkEnv = new SparkEnv("kbits").sparkEnv
    //val inputTable = s"${distanceTable}_1_3"
    val outputTable = s"algo.dxp_simhash_threshold_${thres}"
    val ori_sql: String = "select a.fid, a.hash, a.samelen, b.furl,b.ftitle,b.fkeywords,b.fcontent,c.cat,c.prob from " +
      s" (select fid, hash, samelen from ${distanceTable}_1_3 where stat_date=${statDate} " +
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


  // 找出完全相同的hash值.
  def findSame(statDate: String) = {
    val sparkEnv = new SparkEnv("hash").sparkEnv

    val ori_sql = "select fid,hash " +
      s"from ${keyTable} where stat_date=${statDate}"

    val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql
    val data = sparkEnv.sql(sql).rdd.map(r => {
      (r.getAs[Long](1), Array(r.getAs[Long](0)))
    }).reduceByKey(_ ++ _).map(r => {
      (r._1, r._2(0), r._2.mkString(","), r._2.length)
    })

    val cols = Array("hash", "fid", "samefid", "samelen")
    val colsType = Array("long", "long", "string", "int")
    val st = DXPUtils.createSchema(cols.zip(colsType))
    val df: DataFrame = sparkEnv.createDataFrame(data.map(r => {
      Row(r._1, r._2, r._3, r._4)
    }), st)
    DXPUtils.saveDataFrame(df, sameTable, statDate, sparkEnv)
  }

  /*
  k个bit不同
  将hash切成四块，每块16bits. 在保证任意三块相同的情况下进行聚合，再计算haming-distance
  总共会进行四次去重
   */
  def findkBitsDistance(statDate: String, kBits: Int, bId: Int) = {

    //将hash分成四个块，每次取三个块相同
    def moveByte(hash: Long, bId: Int) = {
      bId match {
        case 0 => (hash & 0x0000ffffffffffffL)
        case 1 => (hash & 0xffff0000ffffffffL)
        case 2 => (hash & 0xffffffff0000ffffL)
        case 3 => (hash & 0xffffffffffff0000L)
        case _ => {
          println("bad bId, exit...")
          sys.exit(1)
        }
      }
    }

    //处理输入表与输出表
    val kBitsTable = s"${distanceTable}_${kBits}"
    val (inputTable,outputTable) = bId match {
      case 0 => (sameTable,s"${kBitsTable}_0")
      case 1|2|3 =>{
        (s"${kBitsTable}_${bId-1}", s"${kBitsTable}_${bId}")
      }
      case _ =>{
        println("bad bId, exit...")
        sys.exit(1)
      }
    }

    val sparkEnv = new SparkEnv("kbits").sparkEnv
    val ori_sql: String = "select fid,hash,samelen " +
      s"from ${inputTable} where stat_date=${statDate}"

    val sql: String = if (isDebug) ori_sql + " limit 10000" else ori_sql
    val data = sparkEnv.sql(sql).rdd.map(r => {
      (moveByte(r.getAs[Long](1), bId), Array((r.getAs[Long](0), r.getAs[Long](1),r.getAs[Int](2))))
    }).reduceByKey(_ ++ _).flatMap(r => {
      val arrayBuffer = new ArrayBuffer[(Long, Long, String, Int)]()
      // 每次取前一次中重复次数最高的fid
      var sameArr = r._2.sortWith(_._3 > _._3)
      while (sameArr.length > 0) {
        val w = sameArr(0)
        val wArr = sameArr.filter(m => {
          w._3 >= m._3 && FNVHash.distance(w._2, m._2, kBits)
        })
        //至少含有自身，所以wArr不为空
        arrayBuffer.append((w._1, w._2, wArr.map(_._1).take(50000).mkString(","), wArr.map(_._3).sum))
        sameArr = sameArr.filterNot(s => wArr.contains(s))
      }
      arrayBuffer
    })

    val cols = Array("fid", "hash", "samefid", "samelen")
    val colsType = Array("long", "long", "string", "int")
    val st = DXPUtils.createSchema(cols.zip(colsType))
    val df: DataFrame = sparkEnv.createDataFrame(data.map(r => {
      Row(r._1, r._2, r._3, r._4)
    }), st)
    DXPUtils.saveDataFrame(df, outputTable, statDate, sparkEnv)
  }

  def loadSingleModel(cat: String) = {
    val catFile = s"${cat}.mode.ftz"
    val tmpFile = SparkFiles.get(catFile)
    val m = new FastTextClassifier(tmpFile, cat)
    (cat, m)
  }

}

