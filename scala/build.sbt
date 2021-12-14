val scala3Version = "3.1.0"

lazy val root = project
  .in(file("."))
  .settings(
    name := "pg-tables-watchdog3",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies ++= Seq(
          "com.novocode" % "junit-interface" % "0.11" % "test",
          "org.postgresql" % "postgresql" % "42.3.1",
          "commons-cli" % "commons-cli" % "1.5.0"
    )
  )
