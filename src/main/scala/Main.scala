import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{SaveMode, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {

    val loginStr = s"User login: { Zone: sfsak }"
    val disconnectStr = s"User disconnected: { Zone: sfsak }"
    val logoutStr = s"User logout: { Zone: sfsak }"

    Class.forName("com.mysql.jdbc.Driver").newInstance()
    println("done!")


    val ss: SparkSession = SparkSession.builder.appName("api1").config("spark.master", "local").getOrCreate()
    val sc = ss.sparkContext

    import ss.implicits._
    import DB._

    val dateCfg = ss.readFromSpark("date").select("start", "end", "last")
      .as[DateCfg]
      .take(1)
      .headOption
      .getOrElse(DateCfg.init)

    val fileGroup = getFileList(ss, "/data/", dateCfg.start, dateCfg.end, dateCfg.last)
    fileGroup.foreach{files => {
//      val firstFileName = files(0).getName.split("[.]")(2).replace("-", "")
      val logs = files.sortBy(_.getName).map(f => sc.textFile(f.toUri.getPath))

      val sessions = logs.map(_.filter(l => l.contains(loginStr) || l.contains(disconnectStr) | l.contains(logoutStr)))
        .fold(sc.emptyRDD[String])((o1, o2) => o1 ++ o2)
          .map(l => parseLine(l)).keyBy(_.key).reduceByKey((l1, l2) => {
        Line(0, Math.max(l1.startTime, l2.startTime), Math.max(l1.endTime, l2.endTime),
        l1.uid, l1.smfId, l1.smfSession, Math.max(l1.sessionLength, l2.sessionLength))
      }).map(_._2)

      sessions.toDS().saveToSpark(s"sessions", SaveMode.Append)

      files.lastOption.foreach { p =>
        Seq(dateCfg.copy(last = p.getName.replace("smartfox.log.", ""))).toDS
          .saveToSpark("date", SaveMode.Overwrite) /// update end and last
      }
    }}
  }

  def getFileList(ss: SparkSession, p: String, startDate: String, endDate: String, lastDate: String): List[Array[Path]] = {
    val path = new Path(p)
    val fs = FileSystem.get(ss.sparkContext.hadoopConfiguration)
    val fileList = fs.listStatus(path).filter(_.isFile).map(_.getPath)
      .filter(n => n.getName >= s"smartfox.log.$startDate" && n.getName > s"smartfox.log.$lastDate" && n.getName <= s"smartfox.log.$endDate")
      .sortBy(_.getName)

    fileList.grouped(8).toList
  }

  def parseLine(line: String): Line = {
    val x = line.split(" [|] ")
    val d = DateTime.parse(s"${x(0)} ${x(1)}", DateTimeFormat.forPattern("dd MMM yyyy HH:mm:ss,SSS"))
    val l = x.last.split(", ")
    val tpe = if (l(0).contains("login")) 1 else if (l(0).contains("disconnected")) 2 else 3
    val userIdTmp = l(1).split(": ").last
    val uid = if (userIdTmp.forall(_.isDigit))  userIdTmp.toInt else 0
    val sId = l(2).split(": ").last.toInt
    val session = l(4).split(" ")(1)
    val sessionLength = if (!line.contains("login")) l(5).split(": ").last.toInt else 0
    Line(tpe, if (tpe == 1) d.getMillis else 0, if (tpe != 1) d.getMillis else 0, uid, sId, session, sessionLength)
  }
}


case class Line(tpe: Int, startTime: Long, endTime: Long, uid: Int, smfId: Int, smfSession: String, sessionLength: Int) {
  def key = s"$uid-$smfId-$smfSession"
}

case class DateCfg(start: String, end: String, last: String)

object DateCfg {
  val format = new SimpleDateFormat("yyyy-MM-dd-HH")
  def init = {
    val current = format.format(new Date())
    DateCfg("2018-08-12-00", current, "2018-08-18-00")
  }
}