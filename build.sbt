name := "twitter_analysis"

version := "0.1"

scalaVersion := "2.13.10"

idePackagePrefix := Some("com.github.Areteos")

enablePlugins(JavaAppPackaging)

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-sql" % "3.2.2",
	"org.apache.spark" %% "spark-mllib" % "3.2.2"
)
