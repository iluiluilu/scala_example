import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import java.nio.file.{Files, Paths, StandardCopyOption}

import org.apache.spark.sql.{SaveMode, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {

    val loginStr = s"User login: { Zone: "
    val disconnectStr = s"User disconnected: { Zone: "
    val logoutStr = s"User logout: { Zone: "

    Class.forName("com.mysql.jdbc.Driver").newInstance()
    println("done!")
//    val sparkConfig = new SparkConf().setAppName("api").setMaster("local")
//    val sc:SparkContext = new SparkContext(sparkConfig)

    val ss: SparkSession = SparkSession.builder.appName("api1").getOrCreate()
    val sc = ss.sparkContext

    val fileGroup = getFileList(sc, "/data/", "2019-04-01-00", "2019-04-05-00")
    fileGroup.foreach{files => {
      val firstFileName = files(0).getName.split("[.]")(2).replace("-", "")
      val logs = files.sortBy(_.getName).map(f => sc.textFile(f.toUri.getPath))

      val sessions = logs.map(_.filter(l => l.contains(loginStr) || l.contains(disconnectStr) | l.contains(logoutStr)))
        .fold(sc.emptyRDD[String])((o1, o2) => o1 ++ o2)
          .map(l => parseLine(l)).keyBy(_.key).reduceByKey((l1, l2) => {
        Line(0, Math.max(l1.startTime, l2.startTime), Math.max(l1.endTime, l2.endTime),
        l1.uid, l1.smfId, l1.smfSession, Math.max(l1.sessionLength, l2.sessionLength))
      }).map(_._2)
      //sessions.coalesce(1, false).saveAsTextFile(s"/data/$firstFileName")
      /*Files.move(
        Paths.get(s"rs1/$x/part-00000"),
        Paths.get(s"f/$x"),
        StandardCopyOption.REPLACE_EXISTING
      )*/

      import ss.implicits._
      import DB._
      sessions.toDS().writeDataNotExistTbl(s"sessions", SaveMode.Append)
    }}



//    val path = new Path("data/")
//    val fs = FileSystem.get(sc.hadoopConfiguration)
//    val fileList = fs.listStatus(path).filter(_.isFile).map(_.getPath)
//    fileList.foreach(f => println(f.getName))
//    val logData = fileList.filter(f => f.getName.startsWith("api")).map(f => sc.textFile(f.toUri.getPath)).map(_.filter(l => l.contains("payCardSimGw") || l.contains("PayCardBySimGateWay") || l.contains("simgwCallback")))
//      .fold(sc.emptyRDD[String])((o1, o2) => o1 ++ o2)
//    val callback = logData.filter(f => f.contains("\":18,"))
////     logData.coalesce(1, true).saveAsTextFile("data/result.txt")
//    callback.coalesce(1, true).saveAsTextFile("data/callback.txt")
  }

  def getFileList(sc: SparkContext, p: String, startDate: String, endDate: String): List[Array[Path]] = {
    val path = new Path(p)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val fileList = fs.listStatus(path).filter(_.isFile).map(_.getPath)
      .filter(n => n.getName > s"smartfox.log.$startDate" && n.getName <= s"smartfox.log.$endDate")
      .sortBy(_.getName)

    fileList.grouped(8).toList
  }

  def parseLine(line: String): Line = {
    val x = line.split(" [|] ")
    val d = DateTime.parse(s"${x(0)} ${x(1)}", DateTimeFormat.forPattern("dd MMM yyyy HH:mm:ss,SSS"))
    val l = x.last.split(", ")
    val tpe = if (l(0).contains("login")) 1 else if (l(0).contains("disconnected")) 2 else 3
    val userId = l(1).split(": ").last
    val sId = l(2).split(": ").last
    val session = l(4).split(" ")(1)
    val sessionLength = if (!line.contains("login")) l(5).split(": ").last.toInt else 0
    Line(tpe, if (tpe == 1) d.getMillis else 0, if (tpe != 1) d.getMillis else 0, userId, sId, session, sessionLength)
  }
}


case class Line(tpe: Int, startTime: Long, endTime: Long, uid: String, smfId: String, smfSession: String, sessionLength: Int) {
  def key = s"$uid-$smfId-$smfSession"
}