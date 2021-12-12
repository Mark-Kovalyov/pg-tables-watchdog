package mayton.watchdog.pg

import org.apache.commons.cli.{HelpFormatter, Options}

object CliHelper {

  def printHelp(options: Options) : Unit = {
    val formatter = new HelpFormatter
    formatter.printHelp("pg-watchdog-tables", options)
  }

  def createOptions() : Options = {
    val options : Options = new Options
    options.addOption("h", "host", true, "Host name (default = localhost)")
    options.addOption("p", "port", true, "Port number [0..65535] (default = 5432)")
    options.addOption("d", "database", true, "Database Name")
    options.addOption("u", "username", true, "Username")
    options.addOption("w", "password", true, "Password")
  }

}
