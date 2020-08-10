package com.atguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //数据源
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)

    val host: String = parameterTool.get("host")

    val port: Int = parameterTool.getInt("port")

    val inputDS: DataStream[String] = env.socketTextStream(host,port).setParallelism(1)

    //处理
    val wcDS: DataStream[(String, Int)] = inputDS.flatMap(_.split(" ")).filter(_.nonEmpty).map((_,1)).keyBy(0).sum(1)

    //打印输出
    wcDS.print()

    //启动execution
    env.execute()
  }

}
