package org.cql3scala

import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.BoundStatement

class BoundStatementBuilder(bs: BoundStatement) {

  def this(p: PreparedStatement) = this(new BoundStatement(p))

  def build = bs

  def set[A](col: Column[A], value: A) = {
    col.dataType.bind(col, bs, value)
    this
  }

}

object BoundStatementBuilder {

  implicit def toBoundStatement(bsb: BoundStatementBuilder) = bsb.build

}