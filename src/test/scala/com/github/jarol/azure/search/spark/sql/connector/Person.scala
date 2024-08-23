package com.github.jarol.azure.search.spark.sql.connector

import java.sql.{Date, Timestamp}

case class Person(id: String,
                  creationDate: Date,
                  lastModifiedDate: Timestamp,
                  age: Option[Int])