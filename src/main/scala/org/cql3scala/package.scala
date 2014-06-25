/**
 * *****************************************************************************
 * package.scala
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
package org

import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.Date
import java.util.UUID

import org.cql3scala.CollectionDataType
import org.cql3scala.Column
import org.cql3scala.ContainerType
import org.cql3scala.DataOps
import org.cql3scala.DataType
import org.cql3scala.ElemType
import org.cql3scala.MapDataType
import org.cql3scala.PrimitiveCollectionDataType
import org.cql3scala.PrimitiveDataType
import org.cql3scala.utils.ByteBufferUtil

import com.datastax.driver.core.BoundStatement
import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.QueryBuilder

package object cql3scala {

  type Set[T] = java.util.Set[T]

  type List[T] = java.util.List[T]

  type Map[K, V] = java.util.Map[K, V]

  implicit object SET extends ContainerType[Set]("set") {
    def getCls[E] = classOf[Set[E]]
    def get[E](col: Column[Set[E]], row: Row)(implicit ev: DataType[E]) = row.getSet(col.name, ev.cls)
    def getPrimitive[E <: AnyVal, B](col: Column[Set[E]], row: Row)(implicit ev: PrimitiveDataType[E, B]): Set[E] =
      row.getSet(col.name, ev.boxCls).asInstanceOf[java.util.Set[E]]
    def bind[E](b: BoundStatement, i: Int, value: Set[E]) {
      b.setSet(i, value)
    }
    def bind[E](b: BoundStatement, name: String, value: Set[E]) {
      b.setSet(name, value)
    }
  }

  implicit object LIST extends ContainerType[List]("list") {
    def getCls[E] = classOf[List[E]]
    def get[E](col: Column[List[E]], row: Row)(implicit ev: DataType[E]) = row.getList(col.name, ev.cls)
    def getPrimitive[E <: AnyVal, B](col: Column[List[E]], row: Row)(implicit ev: PrimitiveDataType[E, B]): List[E] =
      row.getList(col.name, ev.boxCls).asInstanceOf[java.util.List[E]]
    def bind[E](b: BoundStatement, i: Int, value: List[E]) {
      b.setList(i, value)
    }
    def bind[E](b: BoundStatement, name: String, value: List[E]) {
      b.setList(name, value)
    }
  }

  implicit def collection[E, C[_]](implicit c: ContainerType[C], e: ElemType[E]) = new CollectionDataType[E, C]

  implicit def primitiveCollection[E <: AnyVal, B, C[_]](implicit c: ContainerType[C], e: PrimitiveDataType[E, B]) =
    new PrimitiveCollectionDataType[E, B, C]

  implicit def map[K, V](implicit kType: ElemType[K], vType: ElemType[V]) = new MapDataType[K, V]

  implicit object INT extends PrimitiveDataType[Int, java.lang.Integer]("int") {
    val cls = classOf[Int]
    val boxCls = classOf[java.lang.Integer]
    def get(col: Column[Int], row: Row) = row.getInt(col.name)
    def bind(b: BoundStatement, i: Int, value: Int) {
      b.setInt(i, value)
    }
    def bind(b: BoundStatement, name: String, value: Int) {
      b.setInt(name, value)
    }
  }

  implicit object BIGINT extends PrimitiveDataType[Long, java.lang.Long]("bigint") {
    val cls = classOf[Long]
    val boxCls = classOf[java.lang.Long]
    def get(col: Column[Long], row: Row) = row.getLong(col.name)
    def bind(b: BoundStatement, i: Int, value: Long) {
      b.setLong(i, value)
    }
    def bind(b: BoundStatement, name: String, value: Long) {
      b.setLong(name, value)
    }
  }

  implicit object TEXT extends DataType[String]("text") with ElemType[String] {
    val cls = classOf[String]
    def get(col: Column[String], row: Row) = row.getString(col.name)
    def bind(b: BoundStatement, i: Int, value: String) {
      b.setString(i, value)
    }
    def bind(b: BoundStatement, name: String, value: String) {
      b.setString(name, value)
    }
  }

  implicit object BOOLEAN extends PrimitiveDataType[Boolean, java.lang.Boolean]("boolean") {
    val cls = classOf[Boolean]
    val boxCls = classOf[java.lang.Boolean]
    def get(col: Column[Boolean], row: Row) = row.getBool(col.name)
    def bind(b: BoundStatement, i: Int, value: Boolean) {
      b.setBool(i, value)
    }
    def bind(b: BoundStatement, name: String, value: Boolean) {
      b.setBool(name, value)
    }
  }

  implicit object BLOB extends DataType[Array[Byte]]("blob") with ElemType[Array[Byte]] {
    val cls = classOf[Array[Byte]]
    def get(col: Column[Array[Byte]], row: Row) =
      ByteBufferUtil.getArray(row.getBytesUnsafe(col.name))
    override def getOption(col: Column[Array[Byte]], row: Row) =
      Option(row.getBytesUnsafe(col.name)).map(ByteBufferUtil.getArray)
    override def qbEq(col: Column[Array[Byte]], value: Array[Byte]) = QueryBuilder.eq(col.name, c(value))
    override def qbGt(col: Column[Array[Byte]], value: Array[Byte]) = QueryBuilder.gt(col.name, c(value))
    override def qbGte(col: Column[Array[Byte]], value: Array[Byte]) = QueryBuilder.gte(col.name, c(value))
    override def qbLt(col: Column[Array[Byte]], value: Array[Byte]) = QueryBuilder.lt(col.name, c(value))
    override def qbLte(col: Column[Array[Byte]], value: Array[Byte]) = QueryBuilder.lte(col.name, c(value))
    override def qbIn(col: Column[Array[Byte]], value: Seq[Array[Byte]]) =
      QueryBuilder.in(col.name, value.map(arr => c(arr): Object): _*)
    override def qbSet(col: Column[Array[Byte]], value: Array[Byte]) = QueryBuilder.set(col.name, c(value))
    def bind(b: BoundStatement, i: Int, value: Array[Byte]) {
      b.setBytesUnsafe(i, c(value))
    }
    def bind(b: BoundStatement, name: String, value: Array[Byte]) {
      b.setBytesUnsafe(name, c(value))
    }
    override def associate(name: String, value: Array[Byte]) = (name, c(value))
    private def c(value: Array[Byte]) = ByteBuffer.wrap(value)
  }

  implicit object BlobAsByteBuffer extends DataOps[Array[Byte], ByteBuffer] {
    def get(col: Column[Array[Byte]], row: Row) = row.getBytesUnsafe(col.name)
    def bind(b: BoundStatement, i: Int, value: ByteBuffer) {
      b.setBytesUnsafe(i, value)
    }
    def bind(b: BoundStatement, name: String, value: ByteBuffer) {
      b.setBytesUnsafe(name, value)
    }
  }

  implicit object TIMESTAMP extends DataType[Date]("timestamp") with ElemType[Date] {
    val cls = classOf[Date]
    def get(col: Column[Date], row: Row) = row.getDate(col.name)
    def bind(b: BoundStatement, i: Int, value: Date) {
      b.setDate(i, value)
    }
    def bind(b: BoundStatement, name: String, value: Date) {
      b.setDate(name, value)
    }
  }

  implicit object TIMEUUID extends DataType[UUID]("timeuuid") with ElemType[UUID] {
    val cls = classOf[UUID]
    def get(col: Column[UUID], row: Row) = row.getUUID(col.name)
    def bind(b: BoundStatement, i: Int, value: UUID) {
      b.setUUID(i, value)
    }
    def bind(b: BoundStatement, name: String, value: UUID) {
      b.setUUID(name, value)
    }
  }

  implicit object FLOAT extends PrimitiveDataType[Float, java.lang.Float]("float") {
    val cls = classOf[Float]
    val boxCls = classOf[java.lang.Float]
    def get(col: Column[Float], row: Row) = row.getFloat(col.name)
    def bind(b: BoundStatement, i: Int, value: Float) {
      b.setFloat(i, value)
    }
    def bind(b: BoundStatement, name: String, value: Float) {
      b.setFloat(name, value)
    }
  }

  implicit object DOUBLE extends PrimitiveDataType[Double, java.lang.Double]("double") {
    val cls = classOf[Double]
    val boxCls = classOf[java.lang.Double]
    def get(col: Column[Double], row: Row) = row.getDouble(col.name)
    def bind(b: BoundStatement, i: Int, value: Double) {
      b.setDouble(i, value)
    }
    def bind(b: BoundStatement, name: String, value: Double) {
      b.setDouble(name, value)
    }
  }

  implicit object DECIMAL extends DataType[java.math.BigDecimal]("decimal") with ElemType[java.math.BigDecimal] {
    val cls = classOf[java.math.BigDecimal]
    def get(col: Column[java.math.BigDecimal], row: Row) = row.getDecimal(col.name)
    def bind(b: BoundStatement, i: Int, value: java.math.BigDecimal) {
      b.setDecimal(i, value)
    }
    def bind(b: BoundStatement, name: String, value: java.math.BigDecimal) {
      b.setDecimal(name, value)
    }
  }

  implicit object VARINT extends DataType[java.math.BigInteger]("varint") with ElemType[java.math.BigInteger] {
    val cls = classOf[java.math.BigInteger]
    def get(col: Column[java.math.BigInteger], row: Row) = row.getVarint(col.name)
    def bind(b: BoundStatement, i: Int, value: java.math.BigInteger) {
      b.setVarint(i, value)
    }
    def bind(b: BoundStatement, name: String, value: java.math.BigInteger) {
      b.setVarint(name, value)
    }
  }

  object COUNTER extends PrimitiveDataType[Long, java.lang.Long]("counter") {
    val cls = classOf[Long]
    val boxCls = classOf[java.lang.Long]
    def get(col: Column[Long], row: Row) = row.getLong(col.name)
    def bind(b: BoundStatement, i: Int, value: Long) {
      b.setLong(i, value)
    }
    def bind(b: BoundStatement, name: String, value: Long) {
      b.setLong(name, value)
    }
  }

}
