import java.io.File
import java.nio.file.{FileSystems, Files, Path}
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.io.Source


object SensorStatisticTask {

  def main(args: Array[String]): Unit = {
    val sensorStatisticTask = new SensorStatisticTask();
    val path = args.head
    val files = sensorStatisticTask.extractFilesFromPath(path)
    val filesContent: Seq[(String, Option[Int])] = files.map(sensorStatisticTask.extractContentFromFile).flatten
    sensorStatisticTask.aggregateResult(filesContent)
  }
}

class SensorStatisticTask {


  def extractFilesFromPath(path: String): Seq[File] = {
    val dir: Path = FileSystems.getDefault.getPath(path)
    val files: Seq[Path] = Files.list(dir).iterator().asScala.toSeq
    println(s"Num of processed files: ${files.size}")
    files.map(x => new File(x.toString))
  }

  def extractContentFromFile(file: File): Seq[(String, Option[Int])] = {
    Source
      .fromFile(file)
      .getLines()
      .drop(1)
      .toSeq
      .map(_.split(","))
      .map { case Array(k, v) =>
        (k, v.toIntOption)
      }
  }

  def aggregateResult(inputs: Seq[(String, Option[Int])]): Seq[(String, (Int, Option[Int], Option[Int], Option[Int]))] = {
    val hashMap: HashMap[String, (Int, Option[Int], Option[Int], Option[Int])] = new HashMap()

    inputs.foreach { x =>
      val sensor = x._1
      val humOpt = x._2
      val value = hashMap.getOrElse(sensor, (0, None, None, None))
      if (humOpt.isEmpty) hashMap.put(sensor, value)
      else {
        val hum = humOpt.get
        val count = value._1
        val minOpt = value._2
        val avgOpt = value._3
        val maxOpt = value._4
        val newCount = count + 1
        val newMin = if (minOpt.isEmpty) hum else Math.min(minOpt.get, hum)
        val newMax = if (maxOpt.isEmpty) hum else Math.max(maxOpt.get, hum)
        val newAvg = if (avgOpt.isEmpty) hum else (avgOpt.get * count + hum) / newCount
        hashMap.put(sensor, (newCount, Some(newMin), Some(newAvg), Some(newMax)))
      }
    }

    val result = hashMap.toSeq.sortBy(_._2._3)(Ordering[Option[Int]].reverse)

    println(s"Num of processed measurements: ${inputs.size}")
    println(s"Num of failed measurements: ${inputs.filter(_._2.isEmpty).size}\n")

    println("Sensors with highest avg humidity\n")
    println("sensor-id,min,avg,max")

    result.foreach { x =>
      val sensor = x._1
      val min = x._2._2.getOrElse("NaN")
      val avg = x._2._3.getOrElse("NaN")
      val max = x._2._4.getOrElse("NaN")
      println(s"$sensor, $min, $avg, $max")
    }
    result

  }

}