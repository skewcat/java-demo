import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.sql.fieldTypes.Timestamp
import org.apache.commons.lang.time.DateUtils
import org.apache.spark.sql.SparkSession
import org.bson.Document
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, ISODateTimeFormat}

object Test {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().config("spark.driver.memory","5g").master("local[*]").appName("123").getOrCreate()
    val startTime = System.currentTimeMillis()
    val optionsMap = Map("collection" -> "portsTraffic", "database" -> "ion", "uri" -> "mongodb://172.18.135.2:26000,172.18.135.3:26000,172.18.135.4:26000")
    val readConfig = ReadConfig(optionsMap)
//    val customDF = MongoSpark.load(sparkSession, readConfig)
    val endTime = System.currentTimeMillis()

    val SIMPLE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"
    val newTime = DateUtils.addMinutes(new Date(),-10)
    val json = String.format("{$match:{ts:{$gte:'%s'}}}",newTime)
    val json1 = "{$match:{ts:{$gte: '2018-12-18 12:10:40'}}}"
    val pipelineSeq = Seq(Document.parse(json1))

    val test = MongoSpark.builder().sparkSession(sparkSession).options(optionsMap).build().toDF()
    println(s"load time = " + (endTime-startTime))
    test.printSchema()
    test.filter("ts>'2018-12-18 15:10:40'").show()
    sparkSession.close()
  }
}
