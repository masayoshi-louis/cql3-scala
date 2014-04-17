package org.cql3scala

import com.datastax.driver.core.querybuilder.Insert

/**
 * Some QueryBuilder enhancements that need to be imported
 */
object QueryBuilderEx {

  implicit class InsertIntoEx(i: Insert) {
    def values(arr: (String, Any)*) = {
      arr foreach {
        case (c, v) => i.value(c, v)
      }
      i
    }
  }

}