ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "spark-video-course",
    idePackagePrefix := Some("com.murali.spark")
  )


val sparkVersion = "3.5.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion, // %% is used to get scala version of the build, we dont have to specify the version of scala eg spark-core_2.13
  "org.apache.spark" %% "spark-sql" % sparkVersion
)