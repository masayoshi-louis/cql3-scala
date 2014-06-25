/**
 * *****************************************************************************
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
 * ****************************************************************************
 */
package org.cql3scala

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.querybuilder.BindMarker

trait Column[A] {

  val table: TableLike
  val name: String
  val dataType: DataType[A]

  def index = table.columnIndex(this.name)
  def ddl = s"$name $dataType" + (if (isStatic) " static" else "")

  override def toString = name

  def apply()(implicit row: Row) = row %: this

  def apply[B](row: Row)(implicit ev: DataOps[A, B]): B = ev.get(this, row)

  def ?=(value: A) = dataType.qbEq(this, value)
  def ?=(bm: BindMarker) = dataType.qbEq(this, bm)
  def ?>(value: A) = dataType.qbGt(this, value)
  def ?>(bm: BindMarker) = dataType.qbGt(this, bm)
  def ?>=(value: A) = dataType.qbGte(this, value)
  def ?>=(bm: BindMarker) = dataType.qbGte(this, bm)
  def ?<(value: A) = dataType.qbLt(this, value)
  def ?<(bm: BindMarker) = dataType.qbLt(this, bm)
  def ?<=(value: A) = dataType.qbLte(this, value)
  def ?<=(bm: BindMarker) = dataType.qbLte(this, bm)
  def ?<-*(value: A*) = dataType.qbIn(this, value)
  def ?<-(value: Seq[A]) = dataType.qbIn(this, value)
  def ?<-(bm: BindMarker) = dataType.qbIn(this, bm)
  def :=(value: A) = dataType.qbSet(this, value)
  def :=(bm: BindMarker) = dataType.qbSet(this, bm)

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

  def isStatic = this.isInstanceOf[StaticColumn]

}

object Column {
  implicit def col2Name(c: Column[_]) = c.name
}

trait CounterColumn extends Column[Long] {
  def :++ = QueryBuilder.incr(name)
  def :+=(value: Long) = QueryBuilder.incr(name, value)
  def :+=(bm: BindMarker) = QueryBuilder.incr(name, bm)
}

trait SetColumn[E] extends Column[Set[E]] {
  def :+=(value: E) = QueryBuilder.add(name, value)
  def :+=(bm: BindMarker) = QueryBuilder.add(name, bm)
  def :++=(values: Set[E]) = QueryBuilder.addAll(name, values)
  def :++=(bm: BindMarker) = QueryBuilder.addAll(name, bm)
  def :-=(value: E) = QueryBuilder.remove(name, value)
  def :-=(bm: BindMarker) = QueryBuilder.remove(name, bm)
  def :--=(values: Set[E]) = QueryBuilder.removeAll(name, values)
  def :--=(bm: BindMarker) = QueryBuilder.removeAll(name, bm)
}

trait ListColumn[E] extends Column[List[E]] {
  def update(i: Int, v: E) = QueryBuilder.setIdx(name, i, v)
  def update(i: Int, bm: BindMarker) = QueryBuilder.setIdx(name, i, bm)
  def :+=(value: E) = QueryBuilder.append(name, value)
  def :+=(bm: BindMarker) = QueryBuilder.append(name, bm)
  def :++=(values: List[E]) = QueryBuilder.appendAll(name, values)
  def :++=(bm: BindMarker) = QueryBuilder.appendAll(name, bm)
  def :-=(value: E) = QueryBuilder.discard(name, value)
  def :-=(bm: BindMarker) = QueryBuilder.discard(name, bm)
  def :--=(values: List[E]) = QueryBuilder.discardAll(name, values)
  def :--=(bm: BindMarker) = QueryBuilder.discardAll(name, bm)
  def +=:(value: E) = QueryBuilder.prepend(name, value)
  def +=:(bm: BindMarker) = QueryBuilder.prepend(name, bm)
  def ++=:(values: List[E]) = QueryBuilder.prependAll(name, values)
  def ++=:(bm: BindMarker) = QueryBuilder.prependAll(name, bm)
}

trait PrimaryKey[A] extends Column[A]

trait PartitionKey[A] extends PrimaryKey[A] {
  assert(!this.isInstanceOf[ClusteringKey[_]])
}

trait ClusteringKey[A] extends PrimaryKey[A] {
  assert(!this.isInstanceOf[PartitionKey[_]])
}

trait StaticColumn {
  assert(!this.isInstanceOf[PrimaryKey[_]])
}
