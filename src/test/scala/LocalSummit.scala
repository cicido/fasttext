import org.apache.spark.sql.SparkSession
object LocalSummit {

  def main(args: Array[String]) {
    val sparkEnv = SparkSession
      .builder()
      .master("local[1]")
      .appName("test")
      .enableHiveSupport()
      .getOrCreate()
    val rdd =sparkEnv.sparkContext.parallelize(List(1,2,3,4,5,6)).map(_*3)
    val mappedRDD=rdd.filter(_>10).collect()
    //对集合求和
    println(rdd.reduce(_+_))
    //输出大于10的元素
    for(arg <- mappedRDD)
      print(arg+" ")
    println()
    println("math is work")
  }
}
