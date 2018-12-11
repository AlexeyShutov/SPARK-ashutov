###Spark project on Scala


####Data format
- List of warehouse positions. Each record for particular position is unique. 

     Format: { positionId: Long, warehouse: String, product: String, eventTime: Timestamp (Long) }

- List of amounts. Records for particular position can be repeated. The latest record means current amount. 

     Format: { positionId: Long, amount: BigDecimal, eventTime: Timestamp (Long) }.
 
 Initial DataSet is included. Run DataGenerator to create a new DataSet.