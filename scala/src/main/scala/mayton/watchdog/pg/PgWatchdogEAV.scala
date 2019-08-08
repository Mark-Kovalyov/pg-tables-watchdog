package mayton.watchdog.pg

import java.io.{FileOutputStream, PrintWriter}
import java.sql.Connection

import mayton.watchdog.pg.Utils._

import scala.collection.mutable

object PgWatchdogEAV {

  def EAV_TABLE_PREFIX = "mtn_"

  def createDefaultEAVLog(script : PrintWriter) : Unit = {
    script.println(
      s"""|CREATE TABLE eav_log(
          |  TS          TIMESTAMP   NOT NULL,
          |  OPERATION   CHAR(1)     NOT NULL CHECK IN ('I','U','D'),
          |  TABLE_NAME  VARCHAR(64) NOT NULL,
          |  COLUMN_NAME VARCHAR(64) NOT NULL
          |  VALUE       TEXT)""".stripMargin)
  }

  def eavTriggerScript(script: PrintWriter, tableName: String) : Unit = {

  }

  def process(connection: Connection, script : PrintWriter) : Boolean = {
    for(tableName <- tables(connection)) {
      val columnDefinitions : List[ColumnDefinition] = getColumnNames(connection, tableName)
      //eavTriggerScript(script, tableName)
    }
    script.close()
    true
  }

  def main(arg : Array[String]) : Unit = {
    val props : mutable.Map[String,String] = toScalaMutableMap(tryToLoadSensitiveProperties())
    val host       = props("host")
    val port       = props("port")
    val database   = props("database")
    val connection = createConnection(s"jdbc:postgresql://$host:$port/$database", props("user"), props("password"))
    val script = new PrintWriter(new FileOutputStream("out/pg-watchdog-tables-eav.sql"))
    process(connection, script)
  }

}
