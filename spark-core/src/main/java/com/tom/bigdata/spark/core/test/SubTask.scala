package com.tom.bigdata.spark.core.test

class SubTask extends Serializable {
  var data: List[Int] = _
  var logic: (Int) => Int = _

  // 计算
  def compute() = {
    data.map(logic)
  }
}
