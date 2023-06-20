name := "BelegLogAnalyseAufgabe"

version := "0.1"

scalaVersion := "2.12.16"
val SparkVersion = "3.4.0"
parallelExecution in Test := false

mainClass in (Compile, run) := Some("main.App")

run := Defaults.runTask(fullClasspath in Runtime, mainClass in run in Compile, runner in run).evaluated


libraryDependencies ++=Seq(
	"org.apache.spark" %% "spark-core" % SparkVersion,
	"org.apache.spark" %% "spark-sql" % SparkVersion,
	"org.scalactic" %% "scalactic" % "3.2.5",
	"org.scalatest" %% "scalatest" % "3.2.5" % "test",
	"org.jfree" % "jfreechart" % "1.0.19")

