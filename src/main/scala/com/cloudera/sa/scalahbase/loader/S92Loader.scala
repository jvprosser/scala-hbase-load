package com.cloudera.sa.scalahbase.loader

import java.util.Random
import com.cloudera.sa.impact1.tables.HBaseUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Connection}
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.hbase.spark.{KeyFamilyQualifier, HBaseContext}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.kohsuke.randname.RandomNameGenerator

object S92Loader {

  val schema = StructType(Array(
  StructField("patient_id", StringType, true),
  StructField("name", StringType, true),
  StructField("age", IntegerType, true),
  StructField("gender", StringType, true)))

  val  SN_INX	     = 1
  val  OPTIME_INX    = 2
  val  ID_INX	     = 3
  val  RDFNAME_INX   = 0
  val  PARAMID_INX   = 4
  val  PARAMTYPE_INX = 5
  val  VALID_INX     = 6
  val  TS_INX        = 7
  val  VAL_INX       = 8

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println( "{staging path: e.g. /tmp/importstaging} {path of file to Bulk Load: e.g. /tmp/2015-11-05_10-11-17_para_compressed.tsv }")
    }

    val runLocally = args(2).toLowerCase.equals("l")
    val inputPath =  args(1) 
    val stagingPath =  args(0) 
    val numOfThreads = 1

    val sc: SparkContext = if (runLocally) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      new SparkContext("local[" + numOfThreads + "]", "test", sparkConfig)
    } else {
      val sparkConfig = new SparkConf().setAppName("ConfigurableDataGeneratorMain")
      new SparkContext(sparkConfig)
    }

      val connection = ConnectionFactory.createConnection()
      generateAndPersist( numOfThreads, sc, connection, stagingPath,inputPath)
      connection.close()


    sc.stop()
  }

  def generateAndPersist(numOfThreads: Int,
                         sc: SparkContext,
                         connection: Connection,
                         stagingPath: String,
			 inputPath: String): Unit = {


//    val df = generate(numOfPatients, numOfThreads, sc)

// rdfName SN      OPTIME  ID      PARAMID PARAMTYPE       VALID   TS      VAL

val sqlContext = new SQLContext(sc)
val customSchema = StructType(Array(
    StructField("rdfName"  , StringType, true), 
    StructField("SN"       , StringType, true),
    StructField("OPTIME"   , StringType, true),
    StructField("ID"       , StringType, true),
    StructField("PARAMID"  , StringType, true),
    StructField("PARAMTYPE", StringType, true),
    StructField("VALID"    , StringType, true),
    StructField("TS"       , StringType, true),
    StructField("VAL"      , StringType, true)))



val df = sqlContext.load(
    "com.databricks.spark.csv", 
    schema = customSchema,
    Map("path" -> inputPath, "header" -> "true", "delimiter" -> "\t"))

      println( "Loaded TSV")
//val selectedData = df.select("year", "model")
//selectedData.save("newcars.csv", "com.databricks.spark.csv")

    val hbaseContext = new HBaseContext(sc, connection.getConfiguration)

    val family:Array[Byte] = HBaseUtils.DEFAULT_COLUMN_FAMILY

    // this has to be incremented if this RDF is a continuation file, which means we have to do a lookup
    val i = 1

    hbaseContext.bulkLoad[Row](df.rdd,
      TableName.valueOf("S92"),
      t => {
        val rowKey = Bytes.toBytes(HBaseUtils.generatorSaltedRowKey(t.getString(SN_INX),t.getString(OPTIME_INX),t.getString(ID_INX)))

        val rdfNameVal   =  new KeyFamilyQualifier(rowKey, family, HBaseUtils.getCQ_PARAM_rdfName   (i))
        val PARAMIDVal   =  new KeyFamilyQualifier(rowKey, family, HBaseUtils.getCQ_PARAM_PARAMID   ())
        val PARAMTYPEVal =  new KeyFamilyQualifier(rowKey, family, HBaseUtils.getCQ_PARAM_PARAMTYPE ())
        val VALIDVal     =  new KeyFamilyQualifier(rowKey, family, HBaseUtils.getCQ_PARAM_VALID     (i))
        val TSVal        =  new KeyFamilyQualifier(rowKey, family, HBaseUtils.getCQ_PARAM_TS        (i))
        val VALVal       =  new KeyFamilyQualifier(rowKey, family, HBaseUtils.getCQ_PARAM_VAL       (i))
        val RDFCOUNTVal  =  new KeyFamilyQualifier(rowKey, family, HBaseUtils.getCQ_PARAM_RDFCOUNT  ())

        Seq(
          (rdfNameVal   ,Bytes.toBytes(t.getString(RDFNAME_INX  ))),
          (PARAMIDVal   ,Bytes.toBytes(t.getString(PARAMID_INX  ))),
          (PARAMTYPEVal ,Bytes.toBytes(t.getString(PARAMTYPE_INX))),
          (VALIDVal     ,Bytes.toBytes(t.getString(VALID_INX    ))),
          (TSVal        ,Bytes.toBytes(t.getString(TS_INX       ))),
          (VALVal       ,Bytes.toBytes(t.getString(VAL_INX      ))),
          (RDFCOUNTVal  ,Bytes.toBytes(i))
        ).iterator
      },
      stagingPath)

      println( "about to call LoadIncrementalHfiles")

    val load = new LoadIncrementalHFiles(connection.getConfiguration)

    val htable = new HTable(connection.getConfiguration, TableName.valueOf("S92"))
      println( "About to doBulkLoad")

    load.doBulkLoad(new Path(stagingPath), htable)
          println( "Done with doBulkLoad")

  }


}
