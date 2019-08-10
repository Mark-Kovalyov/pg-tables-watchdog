package mayton.watchdog.pg

import java.io.{FileOutputStream, PrintWriter}
import java.sql.Connection

import mayton.watchdog.pg.PgWatchdogTables.{COL_OPERATION, COL_TS, FUNC_PREFIX, TABLE_PREFIX, createColumnNameCsv}
import mayton.watchdog.pg.Utils._

import scala.collection.mutable

object PgWatchdogEAV {

  def EAV_TABLE_PREFIX = "mtn_"
  def EAV_LOG = "eav_log"

  def createDefaultEAVLog(script : PrintWriter) : Unit = {
    script.println(
      s"""|CREATE TABLE $EAV_LOG (
          |  TS          TIMESTAMP   NOT NULL,
          |  OPERATION   CHAR(1)     NOT NULL CHECK IN ('I','U','D'),
          |  TABLE_NAME  VARCHAR(64) NOT NULL,
          |  COLUMN_NAME VARCHAR(64) NOT NULL
          |  VALUE       TEXT);
          |
          |  """.stripMargin)
  }

  def eavTriggersScript(connection: Connection, script: PrintWriter) : Unit = {
    for(tableName <- tables(connection)) {
      val columnDefinitions : List[ColumnDefinition] = getColumnNames(connection, tableName)
      eavFunctionScript(script, tableName, columnDefinitions)
    }
  }

  def eavFunctionScript(script : PrintWriter, tableName : String, columnDefinitions : List[ColumnDefinition]) : Unit = {

    val cncvl    : String = createColumnNameCsv("", columnDefinitions)
    val cncvlnew : String = createColumnNameCsv("NEW.", columnDefinitions)

    script.print(
      s"""CREATE OR REPLACE FUNCTION $FUNC_PREFIX$tableName() RETURNS TRIGGER AS $$$$
         |BEGIN
         |  IF TG_OP = 'INSERT' THEN
         |    INSERT INTO $EAV_LOG(TS, OPERATION, TABLE_NAME, COLUMN_NAME, VALUE) VALUES(CURRENT_TIMESTAMP, 'I', '$tableName', 'COL1', 'Value1' );
         |    INSERT INTO $EAV_LOG(TS, OPERATION, TABLE_NAME, COLUMN_NAME, VALUE) VALUES(CURRENT_TIMESTAMP, 'I', '$tableName', 'COL2', 'Value2' );
         |    ...............
         |    // TODO:
         |  ELSIF TG_OP = 'UPDATE' THEN
         |    // TODO:
         |  ELSIF TG_OP = 'DELETE' THEN
         |    // TODO:
         |  END IF;
         |RETURN NEW;
         |END;
         |$$$$
         |LANGUAGE plpgsql;
         |
         |""".stripMargin)
  }


  def process(connection: Connection, script : PrintWriter) : Boolean = {
    createDefaultEAVLog(script)
    eavTriggersScript(connection, script)
    true
  }

  def main(arg : Array[String]) : Unit = {
    val props: mutable.Map[String, String] = toScalaMutableMap(tryToLoadSensitiveProperties())
    val host = props("host")
    val port = props("port")
    val database = props("database")
    val connection = createConnection(s"jdbc:postgresql://$host:$port/$database", props("user"), props("password"))
    val script = new PrintWriter(new FileOutputStream("out/pg-watchdog-tables-eav.sql"))
    process(connection, script)
    script.close()
  }

}
