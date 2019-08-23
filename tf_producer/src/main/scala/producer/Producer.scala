package producer

import java.text.DecimalFormat
import java.util
import java.util.Calendar

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import utils.PropertyUtil

import scala.util.Random


/**
  * 模拟数据生产
  * 随机产生：监测点id，车速（按照5分钟的频率变换堵车状态）
  * 序列化为json，然后发送给kafka
  */
object Producer {
  def main(args: Array[String]): Unit = {

    //读取kafka配置信息
    val props = PropertyUtil.properties

    //创建kafka生产者对象
    val producer = new KafkaProducer[String, String](props)

    //模拟生产实时数据，开始时间
    var startTime = Calendar.getInstance().getTimeInMillis / 1000

    //数据模拟（堵车）切换的周期，单位：秒
    val trafficCycle = 300

    val df = new DecimalFormat("0000")
    //开始不停地生产实时数据
    while (true){
      //模拟产生监测点id：1-20
      val randomMonitorId = df.format(Random.nextInt(20) + 1)

      //模拟车速
      var randomSpeed = "000"

      //得到本条数据产生时的当前时间
      val currentTime = Calendar.getInstance().getTimeInMillis / 1000

      //每五分钟切换一次公路状态
      if (currentTime - startTime > trafficCycle){
        randomSpeed = new DecimalFormat("000").format(Random.nextInt(15))
        if (currentTime - startTime > trafficCycle * 2){
          startTime = currentTime
        }
      }else{
        randomSpeed = new DecimalFormat("000").format(Random.nextInt(30) + 30)
      }

      //将产生的数据序列化为json
      val jsonMap = new util.HashMap[String, String]()
      jsonMap.put("monitor_id", randomMonitorId)
      jsonMap.put("speed", randomSpeed)

      //序列化
      val event = JSON.toJSON(jsonMap)
      println(event)

      //发送事件到kafka集群中
      producer.send(new ProducerRecord[String, String](PropertyUtil.getProperty("kafka.topics"), event.toString))
      Thread.sleep(200)
    }
  }

}
