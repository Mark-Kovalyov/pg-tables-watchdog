package mayton.watchdog.pg

import java.io.{File, FileInputStream}
import java.sql.{Connection, DriverManager}
import java.util.Properties
import mayton.watchdog.pg.PgWatchdogTables.TABLE_PREFIX
import org.apache.commons.cli.{CommandLine, Options}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Utils {

  def getColumnNames(connection: Connection, tableName: String) : List[ColumnDefinition] = {
    var listBuffer = ListBuffer[ColumnDefinition]()
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(
      s"""SELECT
         |  ordinal_position,
         |  column_name,
         |  data_type,
         |  character_maximum_length,
         |  is_nullable
         |FROM information_schema.columns WHERE
         |  table_schema   = current_schema()
         |  AND table_name = '$tableName'
         |  ORDER BY ordinal_position
         |
         |  """.stripMargin
    )
    while(resultSet.next()){
      listBuffer += ColumnDefinition(
        resultSet.getString("column_name"),
        resultSet.getString("data_type"),
        resultSet.getInt("character_maximum_length"),
        resultSet.getBoolean("is_nullable")
      )
    }
    resultSet.close()
    statement.close()
    listBuffer.toList
  }

  def tables(connection: Connection) : List[String] =
    var listBuffer = ListBuffer[String]()
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(
      "SELECT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog') AND table_type = 'BASE TABLE'")

    while(resultSet.next()) listBuffer += resultSet.getString("table_name")
    resultSet.close()
    statement.close()
    listBuffer.toList
  end tables

  def createConnection(url : String, user : String, password : String) : Connection =
    val props = new Properties
    props.setProperty("user", user)
    props.setProperty("password", password)
    DriverManager.getConnection(url, props)
  end createConnection

  def tryToLoadSensitiveProperties(commandLine : CommandLine) : Properties = {
    var sensitiveProps : Properties = null
    val resProps = new Properties()

    if (new File("sensitive.properties").exists()) {
      sensitiveProps = new Properties()
      sensitiveProps.load(new FileInputStream("sensitive.properties"))
    }

    // TODO: Fix and implement

    /*for(s <- Array("host", "port", "database", "user", "password")) {
      if (commandLine.hasOption(s)) {
        resProps.put(s, commandLine.getOptionValue(s))
      } else if (sensitiveProps != null && sensitiveProps.contains(s)) {
        resProps.put(s, sensitiveProps.get(s))
      } else {
        println(s"Unable to detect property name $s")
      }
    }
    resProps*/
    sensitiveProps
  }

  def toScalaMutableMap(props : Properties) : mutable.Map[String, String] =
    val map : mutable.Map[String, String] = new mutable.HashMap[String,String]()
    import scala.collection.JavaConverters._
    props.entrySet().asScala.foreach {
      entry => {
        val pkey = entry.getKey.asInstanceOf[String]
        val pvalue = entry.getValue.asInstanceOf[String]
        map += ((pkey, pvalue))
      }
    }
    map
  end toScalaMutableMap
}
