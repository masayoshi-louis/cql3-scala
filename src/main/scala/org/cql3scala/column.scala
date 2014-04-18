/*******************************************************************************
 * column.scala
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

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.QueryBuilder

trait Column[A] {

  val table: TableLike
  val name: String
  val dataType: DataType[A]

  def index = table.columnIndex(this.name)
  def ddl = s"$name $dataType"

  override def toString = name

  def apply()(implicit row: Row) = row %: this

  def apply[B](row: Row)(implicit ev: DataOps[A, B]): B = ev.get(this, row)

  def ?=(value: A) = dataType.qbEq(this, value)
  def ?>(value: A) = dataType.qbGt(this, value)
  def ?>=(value: A) = dataType.qbGte(this, value)
  def ?<(value: A) = dataType.qbLt(this, value)
  def ?<=(value: A) = dataType.qbLte(this, value)
  def ?<-*(value: A*) = dataType.qbIn(this, value)
  def ?<-(value: Seq[A]) = dataType.qbIn(this, value)
  def :=(value: A) = dataType.qbSet(this, value)

  def %:(row: Row): A = dataType.get(this, row)
  def %?:(row: Row): Option[A] = dataType.getOption(this, row)

  def ->:(value: A) = dataType.associate(name, value)

  def ?=~[B](value: B)(implicit ev: DataOps[A, B]) = ev.qbEq(this, value)
  def ?>~[B](value: B)(implicit ev: DataOps[A, B]) = ev.qbGt(this, value)
  def ?>=~[B](value: B)(implicit ev: DataOps[A, B]) = ev.qbGte(this, value)
  def ?<~[B](value: B)(implicit ev: DataOps[A, B]) = ev.qbLt(this, value)
  def ?<=~[B](value: B)(implicit ev: DataOps[A, B]) = ev.qbLte(this, value)
  def ?<-~*[B](value: B*)(implicit ev: DataOps[A, B]) = ev.qbIn(this, value)
  def ?<-~[B](value: Seq[B])(implicit ev: DataOps[A, B]) = ev.qbIn(this, value)
  def :=~[B](value: B)(implicit ev: DataOps[A, B]) = ev.qbSet(this, value)
  /* useless
  def %~:[B](row: Row)(implicit ev: DataOps[A, B]): B = ev.get(this, row)
  def %~?:[B](row: Row)(implicit ev: DataOps[A, B]): Option[B] = ev.getOption(this, row)
  */
  def ~->:[B](value: B)(implicit ev: DataOps[A, B]) = ev.associate(name, value)

}

object Column {
  implicit def col2Name(c: Column[_]) = c.name
}

trait CounterColumn extends Column[Long] {
  def :++ = QueryBuilder.incr(name)
  def :+=(value: Long) = QueryBuilder.incr(name, value)
}

trait SetColumn[E] extends Column[Set[E]] {
  def :+=(value: Any) = QueryBuilder.add(name, value)
  def :++=(values: Set[E]) = QueryBuilder.addAll(name, values)
  def :-=(value: Any) = QueryBuilder.remove(name, value)
  def :--=(values: Set[E]) = QueryBuilder.removeAll(name, values)
}

trait PrimaryKey[A] extends Column[A]

trait PartitionKey[A] extends PrimaryKey[A] {
  assert(!this.isInstanceOf[ClusteringKey[_]])
}

trait ClusteringKey[A] extends PrimaryKey[A] {
  assert(!this.isInstanceOf[PartitionKey[_]])
}
