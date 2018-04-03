name := "CLaims_Data_For_Test_database"

version := "0.1"

scalaVersion := "2.11.4"

resolvers += "Alfresco Maven Repository" at "https://artifacts.alfresco.com/nexus/content/repositories/public/"

val sparkVersion = "2.2.0"
libraryDependencies ++= {
  val sparkVer = "2.1.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer % "provided" withSources()
  )
}

// https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc4
libraryDependencies += "com.ibm.db2.jcc" % "db2jcc4" % "10.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)
