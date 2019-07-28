import java.io.{File, FileInputStream, FileOutputStream, PrintWriter}
import java.sql.{Connection, DriverManager}
import java.util.Properties

import scala.collection.mutable.ListBuffer

// service postgresql restart
//
// postgres@host-name:~$
// postgres=# create database mydb;
// CREATE DATABASE
//
// postgres=# create user myuser with encrypted password '******';
// CREATE ROLE
//
// postgres=# grant all privileges on database mydb to myuser;
// GRANT
//
// psql -h {host} -p {port} -d {dbname} -U {user} -W {pwd}

object PgWatchdogTables {

  val TABLE_PREFIX  = "mtn_log_"
  val FUNC_PREFIX   = "mtn_func_"
  val TRIGG_PREFIX  = "mtn_trig_"
  val COL_TS        = "mtn_ts"
  val COL_OPERATION = "mtn_op"

  def header(script : PrintWriter) : Unit = {
    script.println("")
  }

  def tableScript(script : PrintWriter, tableName : String, columnDefinitions : List[ColumnDefinition]) : Unit = {
    script.print(s"DROP TABLE IF EXISTS $TABLE_PREFIX$tableName CASCADE;\n\n")
    script.print(s"""CREATE TABLE $TABLE_PREFIX$tableName(
                    |  $COL_TS TIMESTAMP,
                    |  $COL_OPERATION CHAR(1)""".stripMargin)


    for(columnDefinition <- columnDefinitions) {
      script.print(",\n")
      script.print(s"  ${columnDefinition.columnName} ${columnDefinition.dataType}")
      if (columnDefinition.dataType == "character varying") {
        script.print(s"(${columnDefinition.characterMaximumLength})")
      }
    }
    script.print(");\n\n")
  }

  /**
   * CREATE [ CONSTRAINT ] TRIGGER name { BEFORE | AFTER | INSTEAD OF } { event [ OR ... ] }
   * ON table_name
   * [ FROM referenced_table_name ]
   * [ NOT DEFERRABLE | [ DEFERRABLE ] [ INITIALLY IMMEDIATE | INITIALLY DEFERRED ] ]
   * [ REFERENCING { { OLD | NEW } TABLE [ AS ] transition_relation_name } [ ... ] ]
   * [ FOR [ EACH ] { ROW | STATEMENT } ]
   * [ WHEN ( condition ) ]
   * EXECUTE { FUNCTION | PROCEDURE } function_name ( arguments )
   */
  def triggerScript(script : PrintWriter, tableName : String) : Unit = {
    script.print(s"DROP TRIGGER IF EXISTS $TRIGG_PREFIX$tableName ON $TABLE_PREFIX$tableName CASCADE;\n\n")
    script.print(s"CREATE TRIGGER $TRIGG_PREFIX$tableName AFTER INSERT OR UPDATE OR DELETE ON $TABLE_PREFIX$tableName FOR EACH ROW EXECUTE PROCEDURE $FUNC_PREFIX$tableName() ;\n\n")
  }

  def createColumnNameCsv(prefix : String, columnDefinitions: List[ColumnDefinition]): String = {
    columnDefinitions.map(x => prefix + x.columnName).mkString(",")
  }

  /**
   * CREATE [ OR REPLACE ] FUNCTION
   * name ( [ [ argmode ] [ argname ] argtype [ { DEFAULT | = } default_expr ] [, ...] ] )
   * [ RETURNS rettype
   * | RETURNS TABLE ( column_name column_type [, ...] ) ]
   * { LANGUAGE lang_name
   * | TRANSFORM { FOR TYPE type_name } [, ... ]
   * | WINDOW
   * | IMMUTABLE | STABLE | VOLATILE | [ NOT ] LEAKPROOF
   * | CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT
   * | [ EXTERNAL ] SECURITY INVOKER | [ EXTERNAL ] SECURITY DEFINER
   * | PARALLEL { UNSAFE | RESTRICTED | SAFE }
   * | COST execution_cost
   * | ROWS result_rows
   * | SET configuration_parameter { TO value | = value | FROM CURRENT }
   * | AS 'definition'
   * | AS 'obj_file', 'link_symbol'
   * } ...
   */
  def functionScript(script : PrintWriter, tableName : String, columnDefinitions : List[ColumnDefinition]) : Unit = {
    
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
  }

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
      listBuffer += new ColumnDefinition(
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

  def main(arg : Array[String]) : Unit = {
    val props = tryToLoadSensitiveProperties()
    val host  = props.getProperty("host")
    val port  = props.getProperty("port")
    val database   = props.getProperty("database")
    val connection = createConnection(s"jdbc:postgresql://$host:$port/$database", props.getProperty("user"), props.getProperty("password"))

    val script = new PrintWriter(new FileOutputStream("out/pg-watchdog-tables.sql"))

    for(tableName <- tables(connection)) {
      val columnDefinitions : List[ColumnDefinition] = getColumnNames(connection, tableName)
      tableScript(script, tableName, columnDefinitions)
      functionScript(script, tableName, columnDefinitions)
      triggerScript(script, tableName)
    }
    script.close()
  }

}
