import org.scalatest.flatspec.AnyFlatSpec

class SensorStatisticTaskTest extends AnyFlatSpec {

  val SensorStatisticTask = new SensorStatisticTask();

  "Sensor Statistic Task" should "show sensor in result set even all measurements are failed " in {
    val data = Seq(
      ("s1", None),
      ("s1", None),
      ("s1", None)
    )
    val expectedResult = Seq(
      ("s1", None, None, None)
    )
    assert(SensorStatisticTask.aggregateResult(data) == expectedResult)

  }

  it should "round up average correctly" in {
    val data = Seq(
      ("s1", Some(3)),
      ("s1", Some(6)),
    )
    val expectedResult = Seq(
      ("s1", Some(3), Some(5), Some(6))
    )
    assert(SensorStatisticTask.aggregateResult(data) == expectedResult)
  }

  it should "round down average correctly" in {
    val data = Seq(
      ("s1", Some(3)),
      ("s1", Some(6)),
      ("s1", Some(10))
    )
    val expectedResult = Seq(
      ("s1", Some(3), Some(6), Some(10))
    )
    assert(SensorStatisticTask.aggregateResult(data) == expectedResult)
  }

  it should "maintain correct intermediate average correctly to avoid rounding error at the end" in {
    val data = Seq(
      ("s1", Some(3)),
      ("s1", Some(6)),
      ("s1", Some(1))
    )
    val expectedResult = Seq(
      ("s1", Some(1), Some(3), Some(6))
    )
    assert(SensorStatisticTask.aggregateResult(data) == expectedResult)
  }

  it should "return empty if no measurement" in {
    val data = Seq()
    val expectedResult = Seq()
    assert(SensorStatisticTask.aggregateResult(data) == expectedResult)
  }

  it should "show correct result with 1 failed measurement" in {
    val data = Seq(
      ("s1", Some(10)),
      ("s1", None),
      ("s1", Some(20))
    )
    val expectedResult = Seq(
      ("s1", Some(10), Some(15), Some(20))
    )
    assert(SensorStatisticTask.aggregateResult(data) == expectedResult)
  }

  it should "show correct result with multiple sensors in descending average humidity" in {
    val data = Seq(
      ("s1", Some(10)),
      ("s1", None),
      ("s1", Some(20)),
      ("s2", Some(10)),
      ("s2", Some(20)),
      ("s2", Some(30)),
      ("s3", Some(100)),
      ("s3", Some(10)),
      ("s4", None),
      ("s4", None),
    )
    val expectedResult = Seq(
      ("s3", Some(10), Some(55), Some(100)),
      ("s2", Some(10), Some(20), Some(30)),
      ("s1", Some(10), Some(15), Some(20)),
      ("s4", None, None, None)
    )
    assert(SensorStatisticTask.aggregateResult(data) == expectedResult)
  }
}
