package prediction

import java.text.SimpleDateFormat

import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import utils.RedisUtil

import scala.collection.mutable.ArrayBuffer

object Prediction {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("TrafficPrediction").setMaster("local[2]"))

    val dateSDF = new SimpleDateFormat("yyyyMMdd")
    val hourMinuteSDF = new SimpleDateFormat("HHmm")
    val userSDF = new SimpleDateFormat("yyyy-MM-dd HH:mm")

    //用于传入的，想要预测是否堵车的日期
    val inputDateString = "2019-06-04 23:30"
    val inputDate = userSDF.parse(inputDateString)

    //得到redis中的fields
    val dayOfInputDate = dateSDF.format(inputDate)
    val hourMinuteOfInputDate = hourMinuteSDF.format(inputDate)

    val dbIndex = 1
    val jedis = RedisUtil.pool.getResource
    jedis.select(dbIndex)


    //想要预测的监测点
    val monitorIDs = Array("0005", "0015")
    val monitorRelations = Map[String, Array[String]](
      "0005" -> Array("0003", "0004", "0005", "0006", "0007"),
      "0015" -> Array("0013", "0014", "0015", "0016", "0017")
    )

    monitorIDs.map(monitorID => {
      val monitorRelationArray = monitorRelations(monitorID)
      val relationInfo = monitorRelationArray.map(monitorID => {
        (monitorID, jedis.hgetAll(dayOfInputDate + "_" + monitorID))
      })

      //装载目标时间点前三分钟的历史数据
      val dataX = ArrayBuffer[Double]()

      for (index <- Range(3, 0, -1)){
        val oneMoment = inputDate.getTime - index * 60 * 1000
        val oneHM = hourMinuteSDF.format(oneMoment)

        for ((k, v) <- relationInfo){
          if (v.containsKey(oneHM)){
            val speedAndCarCount = v.get(oneHM).split("_")
            val valueX = speedAndCarCount(0).toFloat / speedAndCarCount(1).toFloat
            dataX += valueX
          }else {
            dataX += 60.0F
          }
        }
      }

      //加载模型
      val modelPath = jedis.hget("model", monitorID)
      val model = LogisticRegressionModel.load(sc, modelPath)

      //开始数据预测
      val prediction = model.predict(Vectors.dense(dataX.toArray))
      println(monitorID + ",堵车评估值是：" + prediction + "，是否畅通" + (if (prediction > 3) "畅通" else "不畅通"))

      //保存模型
      jedis.hset(inputDateString, monitorID, prediction.toString)
    })
    RedisUtil.pool.returnResource(jedis)
  }
}