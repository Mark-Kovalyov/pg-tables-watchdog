package mayton.watchdog.pg

import mayton.watchdog.pg.CliHelper.createOptions

import java.io.{FileOutputStream, PrintWriter}
import java.sql.{Connection, DriverManager}
import scala.collection.mutable
import mayton.watchdog.pg.Utils._
import org.apache.commons.cli.{CommandLineParser, DefaultParser, Options}

object PgWatchdogTables {

  val TABLE_PREFIX  = "mtn_log_"
  val FUNC_PREFIX   = "mtn_func_"
  val TRIGG_PREFIX  = "mtn_trig_"
  val COL_TS        = "mtn_ts"
  val COL_OPERATION = "mtn_op"

  def header(script : PrintWriter) : Unit =  script.println("")

  def tableScript(script : PrintWriter, tableName : String, cd : List[ColumnDefinition]) : Unit =
    script.print(s"""DROP TABLE IF EXISTS $TABLE_PREFIX$tableName CASCADE;
                    |
                    |CREATE TABLE $TABLE_PREFIX$tableName(
                    |  $COL_TS TIMESTAMP,
                    |  $COL_OPERATION CHAR(1)""".stripMargin)

    for(columnDefinition <- cd)
      script.print(",\n")
      script.print(s"  ${columnDefinition.columnName} ${columnDefinition.dataType}")
      if (columnDefinition.dataType == "character varying")
        script.print(s"(${columnDefinition.characterMaximumLength})")

    script.print(");\n\n")
  end tableScript


  def triggerScript(pw : PrintWriter, tn : String) : Unit =
    pw.print(s"DROP TRIGGER IF EXISTS $TRIGG_PREFIX$tn ON $TABLE_PREFIX$tn CASCADE;\n\n")
    pw.print(s"CREATE TRIGGER $TRIGG_PREFIX$tn AFTER INSERT OR UPDATE OR DELETE ON $tn FOR EACH ROW EXECUTE PROCEDURE $FUNC_PREFIX$tn() ;\n\n")
  end triggerScript

  def createColumnNameCsv(prefix : String, cd: List[ColumnDefinition]): String =
    cd.map(x => prefix + x.columnName).mkString(",")
  end createColumnNameCsv

  def functionScript(script : PrintWriter, tableName : String, columnDefinitions : List[ColumnDefinition]) : Unit =

    val cncvl    : String = createColumnNameCsv("", columnDefinitions)
    val cncvlnew : String = createColumnNameCsv("NEW.", columnDefinitions)

    script.print(
       s"""CREATE OR REPLACE FUNCTION $FUNC_PREFIX$tableName() RETURNS TRIGGER AS $$$$
          |BEGIN
          |  IF TG_OP = 'INSERT' THEN
          |    INSERT INTO $TABLE_PREFIX$tableName($COL_TS, $COL_OPERATION, $cncvl) VALUES(CURRENT_TIMESTAMP, 'I', $cncvlnew);
          |  ELSIF TG_OP = 'UPDATE' THEN
          |    INSERT INTO $TABLE_PREFIX$tableName($COL_TS, $COL_OPERATION, $cncvl) VALUES(CURRENT_TIMESTAMP, 'U', $cncvlnew);
          |  ELSIF TG_OP = 'DELETE' THEN
          |    INSERT INTO $TABLE_PREFIX$tableName($COL_TS, $COL_OPERATION) VALUES(CURRENT_TIMESTAMP, 'D');
          |  END IF;
          |RETURN NEW;
          |END;
          |$$$$
          |LANGUAGE plpgsql;
          |
          |""".stripMargin)

  end functionScript

  def process(connection: Connection, script : PrintWriter) : Boolean =
    for(tableName <- tables(connection))
      val columnDefinitions : List[ColumnDefinition] = getColumnNames(connection, tableName)
      tableScript(script, tableName, columnDefinitions)
      functionScript(script, tableName, columnDefinitions)
      triggerScript(script, tableName)
    end for
    script.close()
    true
  end process

  def main(arg : Array[String]) : Unit =
    val parser : CommandLineParser = new DefaultParser();
    val props : mutable.Map[String,String] = toScalaMutableMap(tryToLoadSensitiveProperties(parser.parse(createOptions(), arg)))
    val host       = props("host")
    val port       = props("port")
    val database   = props("database")
    val connection = createConnection(s"jdbc:postgresql://$host:$port/$database", props("user"), props("password"))
    val script = new PrintWriter(new FileOutputStream("out/pg-watchdog-tables.sql"))
    process(connection, script)
  end main

}
