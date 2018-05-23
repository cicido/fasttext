package com.meizu.algo.classify

import com.github.jfasttext.JFastText
import com.meizu.algo.util.{Segment, StrUtil}

/**
  * Created by linjiang on 2017/10/18.
  */
class FastTextClassifier(val modelFile: String, parent: String) {

  val isSub = !StrUtil.isEmpty(parent) && parent != "all"
  val model = new JFastText()
  model.loadModel(modelFile)

  def predict(text: String): String = {
    if (text == null || text.trim.length == 0) return "unknow"
    val txt = Segment.segment3(text).mkString(" ")
    if (txt.isEmpty) return "unknow"
    val result = model.predict(txt)
    if (result != null) result.replace("__label__", "") else if (!isSub) "unknow" else parent + "_other"
  }

  /**
    * Create by dxp on 2018-04-19
    * 预测时保留概率，以用于文章取舍，即除了选择概率最大的类别同时指定概率阀值
   */
  def predictWithProb(text:String):(String,Double) = {
    if (text == null || text.trim.length == 0) return ("unknow",-1.0)
    val txt = Segment.segment3(text).mkString(" ")
    if (txt.isEmpty) return ("unknow", -1.0)
    val result = model.predictProba(txt)
    if (result != null) {
      val label = result.label.replace("__label__", "")
      val logprob = math.exp(result.logProb.toDouble)
      (label,logprob)
    } else if (!isSub)
      ("unknow",-1.0)
    else (parent + "_other",-1.0)
  }

  def destroy(): Unit = {
    model.unloadModel()
  }
}
