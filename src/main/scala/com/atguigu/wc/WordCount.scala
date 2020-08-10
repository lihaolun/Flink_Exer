package com.atguigu.wc

import org.apache.flink.api.scala._

object WordCount {
  //批处理wordcount
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //数据源
    val inpath = "F:\\WorkSpace\\inteliJ\\FlinkTutorial\\src\\main\\resources\\hello.txt"

    val inputDS: DataSet[String] = env.readTextFile(inpath)

    val wcDS: AggregateDataSet[(String, Int)] = inputDS.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    //打印输出
    wcDS.print()
  }

}
