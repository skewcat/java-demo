import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by pengxingxiong@ruijie.com.cn on 2019/7/2 16:07
  */
object WordCount {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
//    sparkConf.set("spark.driver.host", "localhost")
//      .set("spark.default.parallelism","3")
    val sc = new SparkContext(sparkConf)

    val line = sc.textFile("hdfs://172.18.135.131:9000/user/spark/pengxx/README.md")
    val result = line.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
    result.cache()
    println(result.count())
    println(result.collect.foreach(x => println(x._1 + ":" + x._2)))
    println(SizeEstimator.estimate(result))
    while (true){}
  }
}
