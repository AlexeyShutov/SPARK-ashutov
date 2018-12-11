package inventory

import org.apache.spark.sql.{Dataset, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import inventory.InventoryStatistics._

@RunWith(classOf[JUnitRunner])
class InventoryStatisticsTest extends FunSuite with BeforeAndAfterAll {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Inventory Test")
    .config("spark.master", "local")
    .getOrCreate()

  import sparkSession.implicits._

  var warehouseDs: Dataset[WarehouseRow] = _
  var amountDs: Dataset[AmountRow] = _

  override def beforeAll(): Unit = {
    warehouseDs = sparkSession.sparkContext.parallelize(Seq(
      WarehouseRow(1, "W-1", "P-1", 100000), WarehouseRow(2, "W-1", "P-1", 50000),
      WarehouseRow(3, "W-2", "P-3", 100000), WarehouseRow(4, "W-2", "P-3", 50000)
    )).toDS()
    amountDs = sparkSession.sparkContext.parallelize(Seq(
      AmountRow(1, 12.31, 100000), AmountRow(1, 50.0, 110000), AmountRow(1, 88.88, 90000),
      AmountRow(2, 30.0, 80000), AmountRow(3, 15.8, 50000), AmountRow(4, 90.14, 600000)
    )).toDS()
  }

  override def afterAll(): Unit = {
    sparkSession.stop()
  }

  test("'getCurrentAmountDs' should take latest event's amount") {
    val currentAmounts = getCurrentAmountDs(warehouseDs, amountDs)
    val currentAmountForPosition1: BigDecimal = currentAmounts
      .select("amount")
      .where($"positionId" === 1).head().getDecimal(0)
    assert(50.0 === currentAmountForPosition1, "Wrong current amount")
  }

  test("'getWarehouseStatistics' should take proper MAX Amount") {
    val maxAmount = getWarehouseStatistics(warehouseDs, amountDs)
      .select("maxAmount")
      .where($"warehouse" === "W-1").head().getDecimal(0)
    assert(88.88 === maxAmount.doubleValue(), "Wrong MAX amount")
  }

  test("'getWarehouseStatistics' should take proper MIN Amount") {
    val minAmount = getWarehouseStatistics(warehouseDs, amountDs)
      .select("minAmount")
      .where($"warehouse" === "W-1").head().getDecimal(0)
    assert(12.31 === minAmount.doubleValue(), "Wrong MIN amount")
  }

  test("'getWarehouseStatistics' should take proper Average Amount") {
    val avgAmount = getWarehouseStatistics(warehouseDs, amountDs)
      .select("avgAmount")
      .where($"warehouse" === "W-1").head().getDecimal(0)
    assert((12.31 + 50.0 + 88.88 + 30) / 4 === avgAmount.doubleValue(), "Wrong Average amount")
  }
}
