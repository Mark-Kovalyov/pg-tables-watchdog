name := "pg-watchdog-tables"

version := "0.1"

scalaVersion := "2.13.0"

libraryDependencies += "org.postgresql" % "postgresql" % "42.3.1"

libraryDependencies += "commons-cli" % "commons-cli" % "1.5.0"

mainClass in (Compile, run) := Some("mayton.watchdog.pg.PgWatchdogTables")

//mainClass in (Compile, run) := Some("mayton.watchdog.pg.PgWatchdogEAV")




