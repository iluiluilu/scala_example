import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf().setAppName("api").setMaster("local")
    val sc:SparkContext = new SparkContext(sparkConfig)



    val path = new Path("data/")
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val fileList = fs.listStatus(path).filter(_.isFile).map(_.getPath)

    fileList.foreach(f => println(f.getName))

    val logData = fileList.map(f => sc.textFile(f.toUri.getPath)).map(_.filter(l => l.contains("payCardSimGw") || l.contains("PayCardBySimGateWay") || l.contains("simgwCallback")))
      .fold(sc.emptyRDD[String])((o1, o2) => o1 ++ o2)
    val callback = logData.filter(f => f.contains("simgwCallback") && f.contains("ussd") && "port".r.findAllMatchIn(f).length > 1)
//    logData.coalesce(1, true).saveAsTextFile("data/result.txt")


    callback.coalesce(1, true).saveAsTextFile("data/callback.txt")
  }
}
