package com.cloudera.sa.scalahbase.tables

import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{ConnectionFactory, Connection}
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy
//import scala.collection.immutable.StringOps

object TableSetup {
  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      println("{numOfRegions}, {numOfSalts}")
      return
    }

    val numOfRegions = args(0).toInt
    val numOfSalts = args(1).toInt

    val connection = ConnectionFactory.createConnection()
    createTable(connection, numOfRegions, numOfSalts, "S92")

    connection.close()
  }

  def createTable(connection: Connection, numOfRegions: Int, numOfSalts: Int, tableName: String): Unit = {
    val admin = connection.getAdmin

    val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))

    val columnDescriptor = new HColumnDescriptor("f")
    columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY)
    columnDescriptor.setBlocksize(64 * 1024)
    columnDescriptor.setBloomFilterType(BloomType.ROW)

    tableDescriptor.addFamily(columnDescriptor)

    tableDescriptor.setRegionSplitPolicyClassName(classOf[ConstantSizeRegionSplitPolicy].getName)

    val splitKeys = new Array[Array[Byte]](numOfRegions)

    val saltsPerRegion = numOfSalts / numOfRegions

println("generate splits with salstperegion=" + saltsPerRegion.toString)

    for (i <- 0 until numOfRegions) {
      val startingRowKey = if (i == 0) 0 else i*saltsPerRegion
//      splitKeys(i) = Bytes.toBytes(startingRowKey.toString.padTo(3, '0'))
      splitKeys(i) = Bytes.toBytes(HBaseUtils.DEFAULT_PADDING_STRING.format(startingRowKey))

      println(" k=" + HBaseUtils.DEFAULT_PADDING_STRING.format(startingRowKey) )
    }

    println("Creating TABLE.")
    admin.createTable(tableDescriptor, splitKeys)

    println("Created Table")
    admin.close()
    println("Admin Closed")
  }
}
