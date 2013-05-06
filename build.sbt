import AssemblyKeys._

assemblySettings

name := "ConvExtractService"

version := "0.0.1"

scalaVersion := "2.10.1"

libraryDependencies ++= Seq(
	"org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.12",
	"com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.1.3",
	"org.slf4j" % "slf4j-api" % "1.7.5",
	"ch.qos.logback" % "logback-classic" % "1.0.11",
	"org.apache.hadoop" % "hadoop-core" % "0.20.2"
)
