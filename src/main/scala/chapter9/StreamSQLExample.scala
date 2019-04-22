package chapter9
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Simple example for demonstrating the use of SQL on a Stream Table in Scala.
  *
  * This example shows how to:
  *  - Convert DataStreams to Tables
  *  - Register a Table under a name
  *  - Run a StreamSQL query on the registered Table
  *
  */
object StreamSQLExample {

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
/*
    flink-table not working. I will check when it pass to stable release.
    val tEnv = StreamTableEnvironment.create(env)

    val orderA: DataStream[Order] = env.fromCollection(Seq(
      Order(1L, "beer", 3),
      Order(1L, "diaper", 4),
      Order(3L, "rubber", 2)))

    val orderB: DataStream[Order] = env.fromCollection(Seq(
      Order(2L, "pen", 3),
      Order(2L, "rubber", 3),
      Order(4L, "beer", 1)))

    // convert DataStream to Table
    var tableA = tEnv.fromDataStream(orderA, 'user, 'product, 'amount)
    // register DataStream as Table
    tEnv.registerDataStream("OrderB", orderB, 'user, 'product, 'amount)

    // union the two tables
    val result = tEnv.sqlQuery(
      s"SELECT * FROM $tableA WHERE amount > 2 UNION ALL " +
        "SELECT * FROM OrderB WHERE amount < 2")

    result.toAppendStream[Order].print()
*/
    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Order(user: Long, product: String, amount: Int)

}