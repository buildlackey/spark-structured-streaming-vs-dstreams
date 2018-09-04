scalaVersion := "2.11.12"

version := "0.0.1"


//resolvers += Resolver.url("SparkPackages", url("https://dl.bintray.com/spark-packages/maven/"))
resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"



resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven/"
resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"



// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11/2.3.1
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.1"


// https://mvnrepository.com/artifact/com.holdenkarau/spark-testing-base_2.11/2.3.1_0.10.0
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.3.1_0.10.0" % "test"

// https://oss.sonatype.org/content/groups/public/org/scalatest/scalatest_2.11/3.0.5/
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

libraryDependencies += "org.apache.spark"  %%  "spark-sql"     % "2.3.1"

libraryDependencies += "org.apache.spark"  %%  "spark-sql"     % "2.3.1"

libraryDependencies += "org.apache.spark" %% "spark-hive"  % "2.3.1"

libraryDependencies += "org.apache.spark" %% "spark-graphx"  % "2.3.1"

libraryDependencies += "graphframes" % "graphframes" % "0.5.0-spark2.1-s_2.11"

libraryDependencies += "commons-io"  % "commons-io" % "2.4"



