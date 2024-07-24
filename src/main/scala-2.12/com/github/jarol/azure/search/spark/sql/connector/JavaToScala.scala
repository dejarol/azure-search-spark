package com.github.jarol.azure.search.spark.sql.connector

import java.util
import scala.collection.JavaConverters._

object JavaToScala {

  def listToSeq[T](list: util.List[T]): Seq[T] = list.asScala
}
