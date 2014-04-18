/*******************************************************************************
 * QueryBuilderEx.scala
 * Copyright (c) 2014, masayoshi louis, All rights reserved.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3.0 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.
 ******************************************************************************/
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
