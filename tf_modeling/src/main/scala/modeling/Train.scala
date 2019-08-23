package modeling

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import utils.RedisUtil

import scala.collection.mutable.ArrayBuffer

/**
  * 数学建模
  */
object Train {
  def main(args: Array[String]): Unit = {

    //写入文件的输出流
    val writer = new PrintWriter(new File("model_train.txt"))

    //获取配置
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("TrafficTrain"))

    //定义redis的数据库相关
    val dbIndex = 1

    //获取redis的连接
    val jedis = RedisUtil.pool.getResource

    //选择数据库
    jedis.select(dbIndex)

    //设置目标监测点,选择对哪个监测点进行建模
    val monitorIDs = Array("0005", "0015")

    //取出相关监测点
    val monitorRelations = Map[String, Array[String]](
      "0005" -> Array("0003", "0004", "0005", "0006", "0007"),
      "0015" -> Array("0013", "0014", "0015", "0016", "0017")
    )

    //遍历所有监测点，读取出数据
    monitorIDs.map(monitorID => {
      //得到当前"目标监测点"的相关监测点
      val monitorRelationArray = monitorRelations(monitorID)

      //初始化时间
      val currentDate = Calendar.getInstance().getTime

      //年月日格式化
      val dateSDF = new SimpleDateFormat("yyyyMMdd")

      //当前小时分钟格式化
      val hourMinuteSDF = new SimpleDateFormat("HHmm")

      val dateOfString = dateSDF.format(currentDate)

      //根据"相关监测点"，取出当日的所有监测点的车辆速度与车辆数目的信息，(0003,{1033=93_2, 1032=63_1, 1031=190_2, 1034=140_2, ...})
      val relationInfo = monitorRelationArray.map( monitorID => {
        //@return All the fields and values contained into a hash.
        (monitorID, jedis.hgetAll(dateOfString + "_" + monitorID))
      })

      //确定使用多少小时内的数据进行建模
      val hours = 1
      //创建3个数组。一个数组用于存放特征向量，一个数组存放Label向量，一个数组存放前俩者之间的关联
      val dataX = ArrayBuffer[Double]()
      val dataY = ArrayBuffer[Double]()
      //存放特征向量和特征结果的映射关系
      val dataTrain = ArrayBuffer[LabeledPoint]()

      //将时间拉回一个小时之前，倒序，来回单位：分钟
      for (i <- Range(60 * hours, 2, -1)){
        dataX.clear()
        dataY.clear()

        //以下内容包括：线性滤波
        for (index <- 0 to 2){
          //当前的毫秒数 - 1个小时之前的毫秒数 + 一个小时之后的0分钟、1分钟、2分钟
          val oneMoment = currentDate.getTime - 60 * i * 1000 + index * 60 * 1000
          //当前小时分钟
          val oneHM = hourMinuteSDF.format(new Date(oneMoment))

          //取出该时刻的数据,数据形式(0003,{1033=93_2, 1032=63_1, 1031=190_2, 1034=140_2, ...})
          for ((k, v) <- relationInfo){

            //如果index == 2那么说明里面的数已经全部放在dataX中，那么下一时刻的数据如果是目标卡口的数据需要放在dataY中
            if (k == monitorID && index == 2){
              val nextMoment = oneMoment + 60 * 1000
              val nextHM = hourMinuteSDF.format(new Date(nextMoment))
              if (v.containsKey(nextHM)){
                val speedAndCarCount = v.get(nextHM).split("_")
                val valueY = speedAndCarCount(0).toFloat / speedAndCarCount(1).toFloat
                dataY += valueY
              }
            }

            //判断是否有当前时刻的数据，如果有则保存到特征因子dataX中，如果缺失该部分数据那么默认是-1
            if (v.containsKey(oneHM)){
              val speedAndCarCount = v.get(oneHM).split("_")
              val valueX = speedAndCarCount(0).toFloat / speedAndCarCount(1).toFloat
              dataX += valueX
            }else {
              dataX += -1.0
            }
          }
        }

        //准备训练模型, 将dataX和dataY映射于一个LabeledPoint对象中
        if (dataY.toArray.length == 1){
          val label = dataY.toArray.head
          //label取值范围是：0~15，30~60 -------> 0,1,2,3,4,5,6
          //真实情况如下： 0~120 km/h 车速，划分11个等级
          val record = LabeledPoint(if (label /10 < 6) (label / 10).toInt else 6, Vectors.dense(dataX.toArray))
          dataTrain += record
        }
      }

      //开始组装训练集和测试集
      val rddData = sc.makeRDD(dataTrain)
      val randomSplits = rddData.randomSplit(Array(0.7, 0.3), 11L)
      val trainData = randomSplits(0)
      val testData = randomSplits(1)

      //建立模型
      val model = new LogisticRegressionWithLBFGS().setNumClasses(7).run(trainData)

      //模型预测
      val predictionAndLabel = testData.map{
        case LabeledPoint(label, features) =>
          val prediction = model.predict(features)
          (prediction, label)
      }

      //误差结果
      val metrics = new MulticlassMetrics(predictionAndLabel)
      val accuracy = metrics.accuracy
      writer.write(accuracy + "\n")

      //保存模型
      if (accuracy > 0.7){
        val hdfs_path = "hdfs://hadoop1:8020/tf/model/" +
          monitorID + "_" +
          new SimpleDateFormat("yyyyMMddHHmmss").format(new Date(System.currentTimeMillis()))
        model.save(sc, hdfs_path)
        jedis.hset("model", monitorID, hdfs_path)
      }
    })
//    jedis.close()
    RedisUtil.pool.returnResource(jedis)
    writer.flush()
    writer.close()
  }
}