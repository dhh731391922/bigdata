import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    //创建SparkConf对象
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建spark上下文对象
    val sc = new SparkContext(config)
    println(sc)
    //读取文件，将文件一行一行的读取出来
    //路径查找位置默认从当前部署环境中查找
    //如果需要从本地file:///opt/links
    val lines = sc.textFile("in/1.txt")
    //将单词分解
    val words = lines.flatMap(_.split(" "))
    //为了方便统计将单词的结构改变
    val wordToOne = words.map((_,1))
    //对转换后的数据进行分组聚合
    val wordToSum = wordToOne.reduceByKey(_+_)
    //将统计结果采集后打印到控制台
    val result = wordToSum.collect()
    result.foreach(println)

  }
}
