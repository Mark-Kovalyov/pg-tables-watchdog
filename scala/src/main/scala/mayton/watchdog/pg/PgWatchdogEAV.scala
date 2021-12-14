package mayton.watchdog.pg

import mayton.watchdog.pg.CliHelper.createOptions

import java.io.{FileOutputStream, PrintWriter}
import java.sql.Connection
import mayton.watchdog.pg.Utils._
import org.apache.commons.cli.{CommandLineParser, DefaultParser, Options}

import scala.collection.mutable

object PgWatchdogEAV {

  def EAV_LOG = "eav_log"
  def TRIGGER_SUFFIX = "_trigger"
  def FUNC_SUFFIX = "_func"

  def createDefaultEAVLog(script : PrintWriter) : Unit =
    script.println(
      s"""|CREATE TABLE $EAV_LOG (
          |  TS          TIMESTAMP   NOT NULL,
          |  OPERATION   CHAR(1)     NOT NULL CHECK (OPERATION IN ('I','U','D')),
          |  TABLE_NAME  VARCHAR(64) NOT NULL,
          |  COLUMN_NAME VARCHAR(64) NOT NULL,
          |  COL_VALUE   TEXT);
          |
          |  """.stripMargin)
  end createDefaultEAVLog

  def eavTriggersFunctionScript(connection: Connection, script: PrintWriter) : Unit =
    for(tableName <- tables(connection).filter(p => p != EAV_LOG))
      val columnDefinitions : List[ColumnDefinition] = getColumnNames(connection, tableName)
      eavFunctionScript(script, tableName, columnDefinitions)
  end eavTriggersFunctionScript

  def eavFunctionScript(script : PrintWriter, tableName : String, columnDefinitions : List[ColumnDefinition]) : Unit =

    script.print(
      s"""CREATE OR REPLACE FUNCTION $tableName$FUNC_SUFFIX() RETURNS TRIGGER AS $$$$
         |BEGIN
         |  IF TG_OP = 'INSERT' THEN
         |""".stripMargin)

    for(columnDefinition <- columnDefinitions)
      script.print(s"    IF NEW.${columnDefinition.columnName} IS NOT NULL THEN\n")
      script.print(s"      INSERT INTO $EAV_LOG(TS, OPERATION, TABLE_NAME, COLUMN_NAME, COL_VALUE) VALUES(CURRENT_TIMESTAMP, 'I', '$tableName', '${columnDefinition.columnName}', NEW.${columnDefinition.columnName});\n")
      script.print(s"    END IF;\n")


    script.printf("  ELSIF TG_OP = 'UPDATE' THEN\n")

    for(columnDefinition <- columnDefinitions)
      script.print(s"    IF NEW.${columnDefinition.columnName} IS NOT NULL THEN\n")
      script.print(s"      INSERT INTO $EAV_LOG(TS, OPERATION, TABLE_NAME, COLUMN_NAME, COL_VALUE) VALUES(CURRENT_TIMESTAMP, 'U', '$tableName', '${columnDefinition.columnName}', NEW.${columnDefinition.columnName});\n")
      script.print(s"    END IF;\n")

    script.printf("  ELSIF TG_OP = 'DELETE' THEN\n")

    for(columnDefinition <- columnDefinitions)
      script.print(s"    IF NEW.${columnDefinition.columnName} IS NOT NULL THEN\n")
      script.print(s"      INSERT INTO $EAV_LOG(TS, OPERATION, TABLE_NAME, COLUMN_NAME, COL_VALUE) VALUES(CURRENT_TIMESTAMP, 'D', '$tableName', '${columnDefinition.columnName}', NEW.${columnDefinition.columnName});\n")
      script.print(s"    END IF;\n")

    script.print(
     s"""|  END IF;
         |RETURN NEW;
         |END;
         |$$$$
         |LANGUAGE plpgsql;
         |
         |""".stripMargin)

    script.print(
      s"""
         |CREATE TRIGGER $tableName$TRIGGER_SUFFIX AFTER UPDATE OR INSERT OR DELETE
         |   ON $tableName
         |   FOR EACH ROW EXECUTE PROCEDURE $tableName$FUNC_SUFFIX();
         |
         |""".stripMargin)

  end eavFunctionScript


  def process(connection: Connection, script : PrintWriter) : Boolean =
    createDefaultEAVLog(script)
    eavTriggersFunctionScript(connection, script)
    true


  def main(arg : Array[String]) : Unit =
    val parser : CommandLineParser = new DefaultParser()
    val props = tryToLoadSensitiveProperties(parser.parse(createOptions(), arg))
    val host       = props.get("host")
    val port       = props.get("port")
    val database   = props.get("database")
    val connection = createConnection(s"jdbc:postgresql://$host:$port/$database", props.get("user").toString, props.get("password").toString)
    val script = new PrintWriter(new FileOutputStream("out/pg-watchdog-tables-eav.sql"))
    process(connection, script)
    script.close()
  end main

}
