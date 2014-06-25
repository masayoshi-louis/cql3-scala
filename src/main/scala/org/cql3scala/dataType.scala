/**
 * *****************************************************************************
 * dataType.scala
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

import java.util.Date
import java.util.UUID
import com.datastax.driver.core.Row
import org.cql3scala.utils.ByteBufferUtil
import com.datastax.driver.core.querybuilder.QueryBuilder
import java.nio.ByteBuffer
import scala.collection.JavaConversions._
import com.datastax.driver.core.BoundStatement
import com.datastax.driver.core.querybuilder.BindMarker

abstract class AbstractType {
  val name: String
  override def toString = name
  override def equals(other: Any) = other match {
    case that: AbstractType => this.name == that.name
    case _ => false
  }
  override def hashCode = 41 + name.hashCode
}

trait DataOps[A, B] {
  def get(col: Column[A], row: Row): B
  def getOption(col: Column[A], row: Row): Option[B] = Option(get(col, row))
  def qbEq(col: Column[A], value: B) = QueryBuilder.eq(col.name, value)
  def qbEq(col: Column[A], bm: BindMarker) = QueryBuilder.eq(col.name, bm)
  def qbGt(col: Column[A], value: B) = QueryBuilder.gt(col.name, value)
  def qbGt(col: Column[A], bm: BindMarker) = QueryBuilder.gt(col.name, bm)
  def qbGte(col: Column[A], value: B) = QueryBuilder.gte(col.name, value)
  def qbGte(col: Column[A], bm: BindMarker) = QueryBuilder.gte(col.name, bm)
  def qbLt(col: Column[A], value: B) = QueryBuilder.lt(col.name, value)
  def qbLt(col: Column[A], bm: BindMarker) = QueryBuilder.lt(col.name, bm)
  def qbLte(col: Column[A], value: B) = QueryBuilder.lte(col.name, value)
  def qbLte(col: Column[A], bm: BindMarker) = QueryBuilder.lte(col.name, bm)
  def qbIn(col: Column[A], value: Seq[B]) = QueryBuilder.in(col.name, value.asInstanceOf[Seq[Object]]: _*)
  def qbIn(col: Column[A], bm: BindMarker) = QueryBuilder.in(col.name, bm)
  def qbSet(col: Column[A], value: B) = QueryBuilder.set(col.name, value)
  def qbSet(col: Column[A], bm: BindMarker) = QueryBuilder.set(col.name, bm)
  def bind(b: BoundStatement, i: Int, value: B)
  def bind(b: BoundStatement, name: String, value: B)
  def associate(name: String, value: B): (String, Any) = (name, value)
  def associate(name: String, bm: BindMarker): (String, Any) = (name, bm)
}

abstract class DataType[A](val name: String) extends AbstractType with DataOps[A, A] {
  val cls: Class[A]
}

trait ElemType[A] extends DataType[A]

abstract class PrimitiveDataType[A <: AnyVal, B](name: String) extends DataType[A](name) with ElemType[A] {
  val boxCls: Class[B]
  override def getOption(col: Column[A], row: Row): Option[A] =
    if (row.isNull(col.name)) None else Some(get(col, row))
}

abstract class ContainerType[C[_]](val name: String) extends AbstractType {
  def getCls[E]: Class[C[E]]
  def get[E](col: Column[C[E]], row: Row)(implicit ev: DataType[E]): C[E]
  def getPrimitive[E <: AnyVal, B](col: Column[C[E]], row: Row)(implicit ev: PrimitiveDataType[E, B]): C[E]
  def bind[E](b: BoundStatement, i: Int, value: C[E])
  def bind[E](b: BoundStatement, name: String, value: C[E])
}

class CollectionDataType[E, C[_]](implicit val containerType: ContainerType[C], val elemType: ElemType[E])
  extends DataType[C[E]](s"""$containerType<$elemType>""") {
  val cls = containerType.getCls[E]
  def get(col: Column[C[E]], row: Row) = containerType.get(col, row)
  def bind(b: BoundStatement, i: Int, value: C[E]) { containerType.bind(b, i, value) }
  def bind(b: BoundStatement, name: String, value: C[E]) { containerType.bind(b, name, value) }
  def bindElem(b: BoundStatement, i: Int, value: E) { elemType.bind(b, i, value) }
  def bindElem(b: BoundStatement, name: String, value: E) { elemType.bind(b, name, value) }
}

class PrimitiveCollectionDataType[E <: AnyVal, B, C[_]](implicit containerType: ContainerType[C], elemType: PrimitiveDataType[E, B])
  extends CollectionDataType[E, C] {
  override def get(col: Column[C[E]], row: Row) = containerType.getPrimitive(col, row)
}

class MapDataType[K, V](implicit val kType: ElemType[K], val vType: ElemType[V])
  extends DataType[Map[K, V]](s"""map<$kType,$vType>""") {

  val cls = classOf[Map[K, V]]

  def get(col: Column[Map[K, V]], row: Row) = {
    row.getMap(col.name, kcls, vcls).asInstanceOf[Map[K, V]]
  }

  def bind(b: BoundStatement, i: Int, value: Map[K, V]) {
    b.setMap(i, value)
  }

  def bind(b: BoundStatement, name: String, value: Map[K, V]) {
    b.setMap(name, value)
  }

  private[this] val kcls = getCls(kType)
  private[this] val vcls = getCls(vType)

  private[this] def getCls(t: DataType[_]) = t match {
    case p: PrimitiveDataType[_, _] => p.boxCls
    case r => r.cls
  }

}
