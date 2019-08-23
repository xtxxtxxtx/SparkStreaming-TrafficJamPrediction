package utils

import redis.clients.jedis._

/**
  * redis的相关配置
  */
object RedisUtil {

  //配置redis基本连接参数
  val host = "192.168.2.162"
  val port = 6379
  val timeout = 30000   //超时时间

  val config = new JedisPoolConfig

  //允许最大的连接个数
  config.setMaxTotal(200)
  //最大空闲连接数
  config.setMaxIdle(50)
  //最小空闲连接数
  config.setMinIdle(8)
  //连接时的最大等待毫秒数
  config.setMaxWaitMillis(10000)
  //获取连接时，是否检查连接的有效性
  config.setTestOnBorrow(true)
  //释放连接时到连接池中是否检查有效性
  config.setTestOnReturn(true)
  //连接空闲时，是否检查连接有效性
  config.setTestWhileIdle(true)
  //俩个扫描之间的时间间隔毫秒数
  config.setTimeBetweenEvictionRunsMillis(30000)
  //每个扫描最多的对象数
  config.setNumTestsPerEvictionRun(10)
  //逐出连接最小的空闲时间，默认是180000（30分钟）
  config.setMinEvictableIdleTimeMillis(60000)

  //懒加载
  lazy val pool = new JedisPool(config, host, port, timeout)

  //释放连接
  lazy val hook = new Thread{
    override def run(): Unit = {
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook)

}
