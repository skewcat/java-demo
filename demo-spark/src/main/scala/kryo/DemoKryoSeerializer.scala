package kryo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by pengxingxiong@ruijie.com.cn on 2019/7/2 10:23
  */

//case class Info(name: String, age: Int, gender: String, addr: String, money: Long,number:Long)

object DemoKryoSeerializer {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("KyroTest")
    conf.set("spark.driver.host", "localhost")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", classOf[ToKryoRegistrator].getName) //在Kryo序列化库中注册自定义的类集合
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "2g")
    conf.set("spark.rdd.compress", "true")
    //    conf.registerKryoClasses(Array(classOf[Person]))
    val sc = new SparkContext(conf)

    val arr = new ArrayBuffer[Person]()

    val nameArr = Array[String]("lsw", "yyy", "lss")
    val genderArr = Array[String]("male", "female")
    val addressArr = Array[String]("beijing", "shanghai", "shengzhen", "wenzhou", "hangzhou")
    val startTime = System.currentTimeMillis()
    for (_ <- 1 to 10000000) {
      val person = new Person()
      person.name = nameArr(Random.nextInt(3))
      person.money = Random.nextLong()
      person.age = Random.nextInt(100)
      person.gender = genderArr(Random.nextInt(2))
      person.addr = addressArr(Random.nextInt(5))
      person.number = Random.nextLong()
      arr.+=(person)
    }
    val rdd = sc.parallelize(arr)
    //序列化的方式将rdd存到内存
    rdd.persist(StorageLevel.MEMORY_ONLY_SER)
    println("the count is " + rdd.count())
    println("the time is " + (System.currentTimeMillis() - startTime))
    while (true) {}
  }
}
