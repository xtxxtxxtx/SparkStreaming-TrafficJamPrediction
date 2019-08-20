package consumer

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.{JSON, TypeReference}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import utils.{PropertyUtil, RedisUtil}

object SparkConsumer {
  def main(args: Array[String]): Unit = {

    //配置信息
    val ssc = new StreamingContext(new SparkContext(new SparkConf().setAppName
    ("TrafficStreaming").setMaster("local[2]")), Seconds(5))

    //检查点机制
    ssc.checkpoint("./ssc/checkpoint")

    //配置kafka参数
    val kafkaParams = Map("metadata.broker.list"-> PropertyUtil.getProperty("metadata.broker.list"))

    //配置主题
    val topics = Set(PropertyUtil.getProperty("kafka.topics"))

    //读取kafka主题中的每一个事件
    val kafkaLineDStream = KafkaUtils.createDirectStream[String, String,
      StringDecoder, StringDecoder](ssc,kafkaParams, topics).map(_._2)

    //解析json字符串
    val event = kafkaLineDStream.map(line => {
      //使用fastjson解析当前事件中封装的数据信息
      val lineJavaMap = JSON.parseObject(line, new TypeReference[java.util.Map[String, String]](){})
      //将java map转换为scala map
      import scala.collection.JavaConverters._
      val lineScalaMap: collection.mutable.Map[String, String] = mapAsScalaMapConverter(lineJavaMap).asScala
      println(lineScalaMap)
      lineScalaMap
    })

    //将每一条数据按照monitor_id聚合、聚合时候每一条数据中的速度叠加，车辆数叠加
    //最终结果展示：（0001，（1360， 30））
    val sumOfSpeedAndCount = event
      .map(e => (e.get("monitor_id").get, e.get("speed").get))
      .mapValues(s => (s.toInt, 1))
      .reduceByKeyAndWindow((t1:(Int, Int), t2:(Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2), Seconds(60), Seconds(60))

    //定义redis数据库的索引号
    val dbIndex = 1

    //将采集到的数据，按照每分钟采集到redis中，用于后边的建模和分析
    sumOfSpeedAndCount.foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        partitionRecords.filter((tuple:(String,(Int, Int))) => tuple._2._2 > 0).foreach(pair => {
          //取出60秒的window中所有聚合之后的数据
          val jedis = RedisUtil.pool.getResource
          val monitorId = pair._1
          val sumOfSpeed = pair._2._1
          val sumOfCarCount = pair._2._2

          /**
            * 俩种情况：
            * 1、数据生产时候，会产生时间戳，流入到kafka事件中
            * 2、实时数据，数据消费的时间、就是数据时间
            */
          val currentTime = Calendar.getInstance().getTime
          val dateSDF = new SimpleDateFormat("yyyyyHHdd") //用于redis的key
          val hourMinuteSDF = new SimpleDateFormat("HHmm") //用于redis的fields

          val hourMinuteTime = hourMinuteSDF.format(currentTime)
          val date = dateSDF.format(currentTime)

          //选择数据库
          jedis.select(dbIndex)
          jedis.hset(date + "_" + monitorId, hourMinuteTime, sumOfSpeed + "_" + sumOfCarCount)
          jedis.close()
        })
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}