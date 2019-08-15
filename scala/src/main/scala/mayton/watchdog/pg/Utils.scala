package mayton.watchdog.pg

import java.io.{File, FileInputStream}
import java.sql.{Connection, DriverManager}
import java.util.Properties

import mayton.watchdog.pg.PgWatchdogTables.TABLE_PREFIX

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

  def tables(connection: Connection) : List[String] = {
    var listBuffer = ListBuffer[String]()
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(
      "SELECT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')")

    while(resultSet.next()) {
      val tableName = resultSet.getString("table_name")
      if (!tableName.startsWith(TABLE_PREFIX)) {
        listBuffer += tableName
      }
    }
    resultSet.close()
    statement.close()
    listBuffer.toList
  }

  def createConnection(url : String, user : String, password : String) : Connection = {
    val props = new Properties
    props.setProperty("user", user)
    props.setProperty("password", password)
    DriverManager.getConnection(url, props)
  }

  def tryToLoadSensitiveProperties() : Properties = {
    val props = new Properties()
    if (new File("sensitive.properties").exists()) {
      props.load(new FileInputStream("sensitive.properties"))
    } else {
      props.put("host",     "localhost")
      props.put("port",     "5432")
      props.put("database", "postgres")
      props.put("user",     "postgres")
      props.put("password", "postgres123")
    }
    props
  }

  def toScalaMutableMap(props : Properties) : mutable.Map[String, String] = {
    var map : mutable.Map[String, String] = new mutable.HashMap[String,String]()
    import scala.collection.JavaConverters._
    props.entrySet().asScala.foreach {
      entry => map += ((entry.getKey.asInstanceOf[String], entry.getValue.asInstanceOf[String]))
    }
    map
  }
}
