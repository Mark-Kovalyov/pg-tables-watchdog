name := "pg-watchdog-tables"

version := "0.1"

scalaVersion := "2.13.0"

libraryDependencies += "org.postgresql" % "postgresql" % "42.2.6"

libraryDependencies += "commons-cli" % "commons-cli" % "1.4"

mainClass in (Compile, run) := Some("mayton.watchdog.pg.PgWatchdogTables")




