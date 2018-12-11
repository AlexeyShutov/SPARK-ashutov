package inventory

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}

object InventoryStatistics {

  import org.apache.spark.sql.{Row, SparkSession}
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types.{DecimalType, _}

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Inventory Statistics")
    .config("spark.master", "local")
    .getOrCreate()

  import sparkSession.implicits._

  def main(args: Array[String]): Unit = {
    try {
      val warehouses: DataFrame =
        readToDs("src/main/resources/inventory/warehouse_data.csv",
          fixedDecimalSchema(Encoders.product[WarehouseRow].schema))
      val amounts: DataFrame =
        readToDs("src/main/resources/inventory/amount_data.csv",
          fixedDecimalSchema(Encoders.product[AmountRow].schema))

      val warehouseDs = warehouses.as[WarehouseRow]
      val amountDs = amounts.as[AmountRow]
      getCurrentAmountDs(warehouseDs, amountDs).show()
      getWarehouseStatistics(warehouseDs, amountDs).show()
    } finally {
      sparkSession.stop()
    }
  }

  def readToDs(resource: String, schema: StructType): DataFrame = {
    sparkSession.read
      .option("header", "true")
      .schema(schema)
      .csv(resource)
  }

  def getCurrentAmountDs(warehouses: Dataset[WarehouseRow], amounts: Dataset[AmountRow]): Dataset[Row] = {
    val currentPositions = amounts
      .groupBy("positionId")
      .agg(max("eventTime").as("eventTime"))

    warehouses.join(
      amounts.join(currentPositions, Seq("positionId", "eventTime")),
      Seq("positionId")
    ).select("positionId", "warehouse", "product", "amount")
      .orderBy("positionId")
  }

  def getWarehouseStatistics(warehouses: Dataset[WarehouseRow], amounts: Dataset[AmountRow]): Dataset[Row] = {
    warehouses.join(
      amounts, Seq("positionId")
    ).groupBy(
      warehouses("warehouse"),
      warehouses("product"))
      .agg(
        max("amount").as("maxAmount"),
        min("amount").as("minAmount"),
        avg("amount").as("avgAmount"))
      .orderBy(
        split($"warehouse", "-")(1).cast(IntegerType),
        split($"product", "-")(1).cast(IntegerType))
  }

  def orderProducts(product: Dataset[Product]): Dataset[Product] = {
    product.orderBy("productCode")
  }

  def fixedDecimalSchema(schema: StructType): StructType = {
    new StructType(
      schema.map {
        case StructField(name, dataType, nullable, metadata) =>
          if (dataType.isInstanceOf[DecimalType])
            StructField(name, new DecimalType(10, 2), nullable, metadata)
          else StructField(name, dataType, nullable, metadata)
      }.toArray
    )
  }
}

case class WarehouseRow(
                         positionId: Long,
                         warehouse: String,
                         product: String,
                         eventTime: Timestamp
                       )

case class AmountRow(
                      positionId: Long,
                      amount: BigDecimal,
                      eventTime: Timestamp
                    )