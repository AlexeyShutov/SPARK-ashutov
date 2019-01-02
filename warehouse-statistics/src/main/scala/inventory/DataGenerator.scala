package inventory

import java.io.{BufferedWriter, FileWriter}
import java.util.concurrent.ThreadLocalRandom

import au.com.bytecode.opencsv.CSVWriter

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.math.BigDecimal.RoundingMode

object DataGenerator {

  private val W = "W-"
  private val P = "P-"
  private val A_DAY_IN_MS = 24 * 60 * 60 * 1000
  private val warehouseHeader = Array("positionId", "warehouse", "product", "eventTime")
  private val amountHeader = Array("positionId", "amount", "eventTime")

  val random: ThreadLocalRandom = ThreadLocalRandom.current()

  def main(args: Array[String]): Unit = {
    generateData()
  }

  def generateData(): Unit = {
    val timestampEnd = System.currentTimeMillis()
    val timestampStart = timestampEnd - A_DAY_IN_MS
    val warehouseRecords: ListBuffer[Array[String]] = new ListBuffer[Array[String]]
    val amountRecords: ListBuffer[Array[String]] = new ListBuffer[Array[String]]
    warehouseRecords += warehouseHeader
    amountRecords += amountHeader

    val maxWarehouseRecords = 10000
    for (i <- 1 to maxWarehouseRecords) {
      val timestamp = random.nextLong(timestampStart, timestampEnd + 1)
      warehouseRecords += generateWarehouseRecord(i, timestamp)
      generateAmountRecords(i, timestamp).foreach(amountRecords += _)
    }

    writeAsCsvFile(warehouseRecords, "src/main/resources/inventory/warehouse_data.csv")
    writeAsCsvFile(amountRecords, "src/main/resources/inventory/amount_data.csv")
  }

  def writeAsCsvFile(records: ListBuffer[Array[String]], filePath: String): Unit = {
    val outFile: BufferedWriter = new BufferedWriter(new FileWriter(filePath))
    val csvWriter: CSVWriter = new CSVWriter(outFile)

    try {
      csvWriter.writeAll(records.asJava)
    } finally {
      outFile.close()
    }
  }

  def randomBigDecimal(origin: Double, bound: Double): BigDecimal = {
    val double = random.nextDouble(origin, bound)
    BigDecimal(double).setScale(2, RoundingMode.HALF_UP)
  }

  private def generateWarehouseRecord(counter: Int, eventTime: Long): Array[String] = {
    val warehouseCode = W + ((counter % 50) + 1)
    val productCode = P + random.nextInt(1, 101).toString
    Array(counter.toString, warehouseCode, productCode, eventTime.toString)
  }

  private def generateAmountRecords(counter: Int, eventTime: Long): ListBuffer[Array[String]] = {
    val maxPositions = random.nextInt(1, 6)
    val timestampEnd = System.currentTimeMillis()
    val timestampStart = timestampEnd - A_DAY_IN_MS
    val amountRecords: ListBuffer[Array[String]] = new ListBuffer[Array[String]]
    val maxAmount = 100.0

    amountRecords += Array(counter.toString, randomBigDecimal(1, maxAmount).toString, eventTime.toString)
    for (_ <-1 to maxPositions) {
      amountRecords += Array(counter.toString, randomBigDecimal(1, maxAmount).toString,
        random.nextLong(timestampStart, timestampEnd).toString)
    }

    amountRecords
  }

}