name := "NBA"

version := "1.0"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2",  // Check for the latest version
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "org.apache.httpcomponents" % "httpclient" % "4.5.13") // Update the version according to your needs
