package com.gl

import com.gl.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.scalalang.typed.{count => typedCount, sum => typedSum}
import org.apache.spark.sql.{Dataset, RecovererDataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.Recoverer
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection // used to generate the schema from a case class
import org.apache.spark.sql.Encoder // Used by spark to generate the schema

import scala.reflect.runtime.universe.TypeTag // used to provide type information of the case class at runtime


import org.apache.spark.sql.types.{StringType, StructField} // to define the schema
import org.apache.spark.sql.catalyst.ScalaReflection // used to generate the schema from a case class

import scala.reflect.runtime.universe.TypeTag // used to provide type information of the case class at runtime
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder // Used by spark to generate the schema

import java.sql.Timestamp

import scala.io.StdIn
 
case class Customer(CustomerID: String,
                    Name: String, 
                    UserName: String, 
                    Email: String, 
                    Phone: String, 
                    Website: String, 
                    Company: String
                    )

case class Address(CustomerID: String,
                   Street: String,
                   Suite: String,
                   City: String,
                   ZipCode: String,
                   Latitude: Double,
                   Longitude: Double
                  )

case class Inventory(
                      ProductID: String,
                      ProductName: String,
                      Quantity: Int,
                      Department: String
                    )

case class Invoice(
                   InvoiceNo: String,
                   Amount: Double,
                   Quantity: Int,
                   InvoiceDate: Timestamp,
                   Status : String,
                   PaymentType: String,
                   CustomerID: String)

case class Order(
                  InvoiceNo: String,
                  ItemPrice: Double,
                  Quantity: Int,
                  ProductID: String,
                  CustomerID: String,
                  InvoiceDate: Timestamp
                )


case class Product(ProductID: String,
                   ProductName: String,
                   Price: Int,
                   Department: String,
                   Color: String,
                   Material: String
                  )

object OrderProcessing extends App {


  def schemaOf[T: TypeTag]: StructType = {
    ScalaReflection
      .schemaFor[T] // this method requires a TypeTag for T
      .dataType
      .asInstanceOf[StructType] // cast it to a StructType, what spark requires as its Schema
  }


  def loadCustomers(spark: SparkSession, filePath:String) = {

    import spark.sqlContext.implicits._

    val dataset = spark.read
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(filePath)
      .as[Customer]

    dataset
  }


  def loadAddresses(spark: SparkSession, filePath:String) = {

    import spark.sqlContext.implicits._

    val dataset = spark.read
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(filePath)
      .as[Address]

    dataset
  }

  def loadProducts(spark: SparkSession, filePath:String) = {

    import spark.sqlContext.implicits._

    val dataset = spark.read
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(filePath)
      .as[Product]

    dataset
  }


  def loadInvoices(spark: SparkSession, filePath:String) = {

    import spark.sqlContext.implicits._

    val dataset = spark.read
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(filePath)
      .as[Invoice]

    dataset
  }


  def loadInventory(spark: SparkSession, filePath:String) = {

    import spark.sqlContext.implicits._

    val dataset = spark.read
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(filePath)
      .as[Inventory]

    dataset
  }


  def loadOrders(spark: SparkSession, filePath:String) = {

    import spark.sqlContext.implicits._

    val dataset = spark.read
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(filePath)
      .as[Order]

    dataset
  }


  def timeit[R](description:String, block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    println(description, " Elapsed time: " + (t1 - t0) + "ms")
    result
  }

  def joinCustomerAndAddress(customersDS:Dataset[Customer],
                             addressDS: Dataset[Address]) = {
    val customersAddress = customersDS.join(addressDS, "CustomerID")
    customersAddress.printSchema()
    customersAddress.show(100)
    customersAddress.explain(true)
  }

  def joinCustomerAndAddressWithBucket(customersDS:Dataset[Customer],
                             addressDS: Dataset[Address]) = {


    val customers_bucket = spark.table("customers_bucket")
    val addresses_bucket = spark.table("addresses_bucket")

    // trigger execution of the join query
    val resultsDS = customers_bucket.join(addresses_bucket, "CustomerID")
    resultsDS.printSchema()
    resultsDS.show(100)
    resultsDS.explain(true)
  }



  def joinCustomerAndAddressWithWithInvoiceBucket(customersDS:Dataset[Customer],
                                       addressDS: Dataset[Address],
                                                  invoiceDS: Dataset[Invoice]) = {




    val customers_bucket = spark.table("customers_bucket")
    val addresses_bucket = spark.table("addresses_bucket")
    val invoice_bucket = spark.table("invoice_bucket")

    // trigger execution of the join query
    val resultsDS = customers_bucket.join(addresses_bucket, "CustomerID")
    resultsDS.cache()

    val resultDS2 = resultsDS.join(invoice_bucket, "CustomerId")

    resultDS2.printSchema()
    resultDS2.show(100)
    resultDS2.explain(true)
  }


  def joinCustomerAndAddressWithWithInvoice(customersDS:Dataset[Customer],
                                                  addressDS: Dataset[Address],
                                                  invoiceDS: Dataset[Invoice]) = {

    // trigger execution of the join query
    val resultsDS = invoiceDS.join(customersDS, "CustomerID")
    //resultsDS.cache()

    val resultDS2 = resultsDS.join(addressDS, "CustomerId")

    resultDS2.printSchema()
    resultDS2.show(100)
    resultDS2.explain(true)
  }

  def createBuckets(customersDS:Dataset[Customer],
                    addressesDS: Dataset[Address],
                    invoicesDS: Dataset[Invoice]) = {

    customersDS.write
      .bucketBy(20, "CustomerID")
      //.sortBy("CustomerID")
      .saveAsTable("customers_bucket")

    addressesDS.write
      .bucketBy(20, "CustomerID")
      // .sortBy("CustomerID")
      .saveAsTable("addresses_bucket")



    invoicesDS.write
      .bucketBy(50, "CustomerID")
      // .sortBy("CustomerID")
      .saveAsTable("invoice_bucket")
  }



  implicit val spark = SparkSession
    .builder
    //.master("local[*]")
    .appName("DS01Basics")
    .config("spark.sql.crossJoin.enabled", "true")
    .config("spark.driver.memory",   "5g")

    .getOrCreate()

    import spark.implicits._

    import spark.sqlContext.implicits._

    spark.sparkContext.setLogLevel("WARN")
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)



  val customersDS = loadCustomers(spark, HdfsUtils.getInputPath("minimal/customers.csv"))
 
    customersDS.printSchema()
    customersDS.show()

  val addressesDS = loadAddresses(spark, HdfsUtils.getInputPath("minimal/addresses.csv"))

  addressesDS.printSchema()
  addressesDS.show()


  val productsDS = loadProducts(spark, HdfsUtils.getInputPath("minimal/products.csv"))

  productsDS.printSchema()
  productsDS.show()


  val inventoryDS = loadInventory(spark, HdfsUtils.getInputPath("minimal/inventory.csv"))

  inventoryDS.printSchema()
  inventoryDS.show()


  val ordersDS = loadOrders(spark, HdfsUtils.getInputPath("minimal/orders.csv"))

  ordersDS.printSchema()
  ordersDS.show()

  val invoicesDS = loadInvoices(spark, HdfsUtils.getInputPath("minimal/invoices.csv"))

  invoicesDS.printSchema()
  invoicesDS.show()


  createBuckets(customersDS, addressesDS, invoicesDS)



  timeit("joinCustomerAndAddressWithBucket",
    joinCustomerAndAddressWithBucket(customersDS, addressesDS))


  timeit("joinCustomerAndAddressWithWithInvoiceBucket",
    joinCustomerAndAddressWithWithInvoiceBucket(customersDS, addressesDS, invoicesDS))

//
//  timeit("Join customer and address",
//    joinCustomerAndAddress(customersDS, addressesDS))
//
//
//
//  timeit("joinCustomerAndAddressWithWithInvoice",
//    joinCustomerAndAddressWithWithInvoice(customersDS, addressesDS, invoicesDS))
//


  StdIn.readLine()

}
