package com.cloudera.sa.scalahbase.tables


import org.apache.hadoop.hbase.util.Bytes
import org.joda.time.{DateTime,DateTimeZone}
import org.joda.time.format.{ISODateTimeFormat , DateTimeFormatter }

object HBaseUtils {

  val DEFAULT_NUM_OF_SALT = 100
  val DEFAULT_PADDING_STRING="%02d"
  val DEFAULT_COLUMN_FAMILY = Bytes.toBytes("f")


// part of rowkey
  def getCQ_PARAM_SN        ()         : Array[Byte] = { Bytes.toBytes("S") }
  def getCQ_PARAM_OPTIME    ()         : Array[Byte] = { Bytes.toBytes("O") }
  def getCQ_PARAM_ID        ()         : Array[Byte] = { Bytes.toBytes("I") }
// end part of rowkey			
					
  def getCQ_PARAM_rdfName   (idx: Int) : Array[Byte] = { Bytes.toBytes("F"+ idx.toString()) }
  def getCQ_PARAM_PARAMID   ()         : Array[Byte] = { Bytes.toBytes("R") }
  def getCQ_PARAM_PARAMTYPE ()         : Array[Byte] = { Bytes.toBytes("Y") }
  def getCQ_PARAM_VALID     (idx: Int) : Array[Byte] = { Bytes.toBytes("D"+ idx.toString()) }
  def getCQ_PARAM_TS        (idx: Int) : Array[Byte] = { Bytes.toBytes("T"+ idx.toString()) }
  def getCQ_PARAM_VAL       (idx: Int) : Array[Byte] = { Bytes.toBytes("V"+ idx.toString()) }
  def getCQ_PARAM_RDFCOUNT  ()         : Array[Byte] = { Bytes.toBytes("X") }

  def generatorSaltedRowKey(sn: String,optime: String,id: String): String = {
    generatorSaltedRowKey(sn,optime,id, DEFAULT_NUM_OF_SALT  )
  }

//2009-02-05T14:07:21.000+0000	

  def generatorSaltedRowKey(sn: String,optime: String,id: String, numOfSalts: Int): String = {
  val formatter: DateTimeFormatter = ISODateTimeFormat.dateTime().withZone(DateTimeZone.getDefault())

  val convOptime: Long = Long.MaxValue - formatter.parseDateTime(optime).getMillis()
  // (Math.abs(sn.hashCode) % numOfSalts).toString.padTo(3, '0') + "|" + sn + "|" + convOptime.toString() + "|" + id
    HBaseUtils.DEFAULT_PADDING_STRING.format( (Math.abs(sn.hashCode) % numOfSalts)) + "|" + sn + "|" + convOptime.toString() + "|" + id
  }

 val PATIENT_NAME = Bytes.toBytes("pN")
  val PATIENT_AGE = Bytes.toBytes("pA")
  val PATIENT_GENDER = Bytes.toBytes("pG")

  val TRANS_DRUG_ID = Bytes.toBytes("tDg")
  val TRANS_DOCTOR_SEQ_INX = Bytes.toBytes("tDr")
  val TRANS_PHARMACY_SEQ_INX = Bytes.toBytes("tPh")
  val TRANS_LENGTH_OF_PRESCRIPTION_INX = Bytes.toBytes("tLp")

  def generatorSaltedRowKey(originalKey: String): String = {
    generatorSaltedRowKey(originalKey, DEFAULT_NUM_OF_SALT  )
  }

  def generatorSaltedRowKey(originalKey: String, numOfSalts: Int): String = {
    (Math.abs(originalKey.hashCode) % numOfSalts).toString.padTo(3, '0') + "|" + originalKey
  }



}
