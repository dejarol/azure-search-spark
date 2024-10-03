package com.github.jarol.azure.search.spark.sql.connector

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.core.config.IOConfig
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession}

import scala.reflect.runtime.universe.TypeTag

/**
 * Trait to mixin for Spark-based tests
 */

trait SparkSpec
  extends BasicSpec {

  protected final lazy val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName(classOf[SparkSpec].getSimpleName)
    .getOrCreate()

  /**
   * Return the Spark schema for a case class
   * @tparam A case class type
   * @return Spark schema that matches the case class model
   */

  protected def schemaOfCaseClass[A <: Product: TypeTag]: StructType = Encoders.product[A].schema

  /**
   * Create a [[DataFrame]] from a collection of case classes.
   *
   * The output dataFrame will have same number of rows as the size of input collection and a schema that matches
   * the structure of such case classes
   * @param data collection of case classes
   * @param columnNames optional column names to assign to output dataFrame
   * @tparam A type of input case classes
   * @return a [[DataFrame]]
   */

  private def toDF[A <: Product: TypeTag](data: Seq[A], columnNames: Option[Seq[String]]): DataFrame = {

    import spark.implicits._
    columnNames match {
      case Some(value) => data.toDF(value: _*)
      case None => data.toDF()
    }
  }

  /**
   * Create a [[DataFrame]] from a collection of case classes, using given column names for schema definition
   * @param data collection of case classes
   * @param colNames collection of column names
   * @tparam A type of case classes
   * @return a [[DataFrame]]
   */

  protected final def toDF[A <: Product: TypeTag](data: Seq[A], colNames: Seq[String]): DataFrame = toDF(data, Some(colNames))

  /**
   * Create a [[DataFrame]] from a collection of case classes
   * @param data collection of case classes
   * @tparam A type of case classes
   * @return a [[DataFrame]]
   */

  protected final def toDF[A <: Product: TypeTag](data: Seq[A]): DataFrame = toDF(data, None)

  /**
   * Create an emtpy DataFrame with same schema as provided case class type
   * @tparam A case class type
   * @return an empty DataFrame
   */

  protected final def emptyDF[A <: Product: TypeTag]: DataFrame = toDF(Seq.empty[A])

  /**
   * Create an emtpy DataFrame with same schema as provided case class type
   * @param colNames column names
   * @tparam A case class type
   * @return an empty DataFrame
   */

  protected final def emptyDF[A <: Product: TypeTag](colNames: Seq[String]): DataFrame = toDF(Seq.empty[A], colNames)

  /**
   * Create an emtpy DataFrame with given schema
   * @return an empty DataFrame
   */

  protected final def emptyDF(schema: StructType): DataFrame = {

    spark.createDataFrame(
      spark.sparkContext.emptyRDD[Row],
      schema
    )
  }

  /**
   * Convert a [[DataFrame]] to a collection of typed instances, using an encoder
   * @param df dataFrame
   * @param encoder encoder for output type
   * @tparam A output type
   * @return a collection of typed instances
   */

  private def toSeqOfUsingEncoder[A](df: DataFrame, encoder: Encoder[A]): Seq[A] = df.as[A](encoder).collect()

  /**
   * Convert a [[DataFrame]] to a collection of case classes
   * @param df dataFrame
   * @tparam A case class type
   * @return a collection of case classes
   */

  protected final def toSeqOf[A <: Product: TypeTag](df: DataFrame): Seq[A] = toSeqOfUsingEncoder[A](df, Encoders.product[A])
}
