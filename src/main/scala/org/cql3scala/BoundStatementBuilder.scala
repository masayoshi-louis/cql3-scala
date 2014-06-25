/**
 * *****************************************************************************
 * BoundStatementBuilder.scala
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
 * ****************************************************************************
 */
package org.cql3scala

import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.BoundStatement

class BoundStatementBuilder(bs: BoundStatement) {

  def this(p: PreparedStatement) = this(new BoundStatement(p))

  def build = bs

  def set[A](col: Column[A], i: Int, value: A) = {
    col.dataType.bind(bs, i, value)
    this
  }

  def set[A](col: Column[A], name: String, value: A) = {
    col.dataType.bind(bs, name, value)
    this
  }

  def setElem[E, C[_]](col: Column[C[E]], i: Int, value: E) = {
    col.dataType.asInstanceOf[CollectionDataType[E, C]].bindElem(bs, i, value)
    this
  }

  def setElem[E, C[_]](col: Column[C[E]], name: String, value: E) = {
    col.dataType.asInstanceOf[CollectionDataType[E, C]].bindElem(bs, name, value)
    this
  }

}

object BoundStatementBuilder {

  implicit def toBoundStatement(bsb: BoundStatementBuilder) = bsb.build

}
