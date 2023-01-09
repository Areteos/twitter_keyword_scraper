name := "twitter_analysis"

version := "0.1"

scalaVersion := "2.13.10"

idePackagePrefix := Some("com.github.Areteos")

enablePlugins(JavaAppPackaging)

libraryDependencies ++= Seq(
//	("org.apache.spark" %% "spark-core" % "3.2.2") cross CrossVersion.for3Use2_13,
//	("org.apache.spark" %% "spark-sql" % "3.2.2") cross CrossVersion.for3Use2_13,
	"org.apache.spark" %% "spark-sql" % "3.2.2",
	"org.apache.spark" %% "spark-mllib" % "3.2.2"
//	"io.github.vincenzobaz" %% "spark-scala3" % "0.1.5"
)

//libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "3.3.1"
//libraryDependencies += "org.json4s" %% "json4s-jackson" % "4.0.6"

//libraryDependencies += "com.danielasfregola" %% "twitter4s" % "8.0"
