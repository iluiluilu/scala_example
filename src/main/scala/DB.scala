import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object DB {
  implicit class ExWriteDb[T](wr: Dataset[T]){
    def saveToSpark(table: String, mode: SaveMode = SaveMode.Overwrite) = wr
      .write
      .format("jdbc")
      .option("url", Cfg.sparkSqlUrl)
      .option("user", Cfg.sparkSqlUser)
      .option("password", Cfg.sparkSqlPass)
      .option("dbtable", s"spark.$table")
      .mode(mode)
      .save()



    def writeDataNotExistTbl(table: String, mode: SaveMode = SaveMode.Overwrite) = {
      wr.write
        .format("jdbc")
        .option("url", Cfg.sparkSqlUrl)
        .option("user", Cfg.sparkSqlUser)
        .option("password", Cfg.sparkSqlPass)
        .option("dbtable", s"spark.$table")
        .mode(mode)
        .saveAsTable(table)
    }
  }

  implicit class ExReader(db: SparkSession) {

    def readFromSpark(table: String)= db.read
      .format("jdbc")
      .option("url", Cfg.sparkSqlUrl)
      .option("user", Cfg.sparkSqlUser)
      .option("password", Cfg.sparkSqlPass)
      .option("dbtable", s"spark.$table")
      .load()
  }
}

object Cfg {
  val sparkSqlUrl = scala.util.Properties.envOrElse("SPARK_URL", "")
  val sparkSqlUser = scala.util.Properties.envOrElse("SPARK_USER", "")
  val sparkSqlPass = scala.util.Properties.envOrElse("SPARK_PASS", "")
}
