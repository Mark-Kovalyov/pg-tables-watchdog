import java.io.{FileOutputStream, PrintWriter}
import java.sql.{Connection, DriverManager}
import java.util.Properties

import scala.collection.mutable.ListBuffer

//type PgColumnDefinition = (String, String)

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
    script.print(s"CREATE TABLE $TABLE_PREFIX$tableName(\n")
    script.print(s"  $COL_TS TIMESTAMP,\n")
    script.print(s"  $COL_OPERATION CHAR(1),\n")
    for(columnDefinition <- columnDefinitions) {
      script.print(s"   ${columnDefinition.columnName} ${columnDefinition.dataType}")
      if (columnDefinition.dataType == "character varying") {
        script.print(s"(${columnDefinition.characterMaximumLength})")
      }
      script.println()
    }
    script.print(");")
    script.print("\n\n")
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
  def triggerScript(script : PrintWriter, tableName : String, columnDefinitions : List[ColumnDefinition]) : Unit = {
    script.print(
      s"CREATE TRIGGER $TRIGG_PREFIX$tableName AFTER INSERT UPDATE DELETE " +
      s"ON $tableName FOR EACH ROW EXECUTE FUNTION $FUNC_PREFIX$tableName ; ")
    script.print("\n\n")
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
    val cncvl : String = createColumnNameCsv("", columnDefinitions)
    val cncvlnew : String = createColumnNameCsv("NEW.", columnDefinitions)
    script.print(
      s"CREATE OR REPLACE FUNCTION $FUNC_PREFIX$tableName() AS $$" +
      s"RETURNS TRIGGER\n" +
      s"BEGIN \n" +
      s"IF TG_OP = 'INSERT' THEN\n" +
      s" INSERT INTO $FUNC_PREFIX$tableName($COL_TS, $COL_OPERATION, $cncvl) VALUES(CURRENT_TIMESTAMP, 'I', $cncvlnew); \n" +
      s"ELSIF TG_OP = 'UPDATE' THEN\n" +
      s" INSERT INTO $FUNC_PREFIX$tableName($COL_TS, $COL_OPERATION, $cncvl) VALUES(CURRENT_TIMESTAMP, 'U', $cncvlnew); \n" +
      s"ELSIF TG_OP = 'DELETE' THEN\n" +
      s" INSERT INTO $FUNC_PREFIX$tableName($COL_TS, $COL_OPERATION) VALUES(CURRENT_TIMESTAMP, 'D'); \n" +
      s"END;\n" +
      s"$$\n" +
      s"LANGUAGE plpgsql;")

    script.print("\n\n")
  }

  def getColumnNames(connection: Connection, tableName: String) : List[ColumnDefinition] = {
    var listBuffer = ListBuffer[ColumnDefinition]()
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(
        s"SELECT " +
        s"  ordinal_position, " +
        s"  column_name, " +
        s"  data_type, " +
        s"  character_maximum_length, " +
        s"  is_nullable " +
        s"FROM information_schema.columns WHERE " +
        s"  table_schema = current_schema() " +
        s"  AND table_name = '$tableName' " +
        s"  ORDER BY ordinal_position"
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
      listBuffer += resultSet.getString("table_name")
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


  def main(arg : Array[String]) : Unit = {

    val host     = "localhost"
    val port     = "5432"
    val database = "<db>"

    val user     = "<user>"
    val password = "<pwd>"

    val connection = createConnection(s"jdbc:postgresql://$host:$port/$database", user, password)

    val script = new PrintWriter(new FileOutputStream("out/pg-watchdog-tables.sql"))

    for(tableName <- tables(connection)) {
      val columnDefinitions : List[ColumnDefinition] = getColumnNames(connection, tableName)
      tableScript(script, tableName, columnDefinitions)
      functionScript(script, tableName, columnDefinitions)
      triggerScript(script, tableName, columnDefinitions)
    }
    script.close()
  }

}
