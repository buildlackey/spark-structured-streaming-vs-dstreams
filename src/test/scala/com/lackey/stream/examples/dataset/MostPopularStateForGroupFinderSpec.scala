package com.lackey.stream.examples.dataset

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.scalatest.{Matchers, WordSpec}

import scala.collection.immutable

class MostPopularStateForGroupFinderSpec extends WordSpec with Matchers {
  "ContinuousTopSensorStateReporter" should {
    "correctly output top states for target sensor" in {

      import MostPopularStateForGroupFinder._

      val t1 = new Timestamp(0, 0, 0, 0, 0, 0, 1)
      val t2 = new Timestamp(0, 0, 0, 0, 0, 0, 2)
      val t3 = new Timestamp(0, 0, 0, 0, 0, 0, 3)

      val data: immutable.Seq[(Timestamp, Iterator[Row])] = List(
        (t1, List[Row]().iterator),
        (t2, List(Row(0, "8", 2L), Row(0, "junk", 2L), Row(0, "fred", 1L)).iterator),
        (t3, List(Row(0, "8", 2L), Row(0, "junk", 2L), Row(0, "fred", 10L)).iterator)
      )

      val expectedResult = List(
        List(),
        List("8", "junk"),
        List("fred")
      )
      val result: immutable.Seq[immutable.Seq[String]] = data.map(findMostPopularState)
      result shouldEqual expectedResult
    }
  }
}