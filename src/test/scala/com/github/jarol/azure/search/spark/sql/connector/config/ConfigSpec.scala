package com.github.jarol.azure.search.spark.sql.connector.config

import com.github.jarol.azure.search.spark.sql.connector.BasicSpec
import org.scalatest.enablers.Emptiness

trait ConfigSpec
  extends BasicSpec {

  /**
   * Emptiness implementation for a [[SearchConfig]]. It allows to write assertions
   * in the form
   * {{{
   *   config should be empty
   * }}}
   * for instances of [[SearchConfig]]
   */

  protected final implicit val searchConfigEmptyNess: Emptiness[SearchConfig] = _.isEmpty

  /**
   * Create an instance of [[ReadConfig]]
   * @param local local options
   * @return a read config
   */

  protected final def readConfig(local: Map[String, String]): ReadConfig = ReadConfig(local, Map.empty)

  /**
   * Create an instance of [[WriteConfig]]
   * @param local global options
   * @return a write config
   */

  protected final def writeConfig(local: Map[String, String]): WriteConfig = WriteConfig(local, Map.empty)
}
