/**
 * *****************************************************************************
 * table.scala
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

import scala.collection.mutable

private[cql3scala] trait TableLike {

  lazy val columnMap: scala.collection.Map[String, Column[_]] = _columns

  lazy val columns = {
    columnMap.values.toIndexedSeq
  }

  lazy val columnNames = columns.map(_.name)

  lazy val columnIndex = columnMap.keysIterator.zipWithIndex.toMap

  lazy val partitionKeys = _partitionKeys.toIndexedSeq

  lazy val clusteringKeys = _clusteringKeys.toIndexedSeq

  lazy val primaryKeys = partitionKeys ++ clusteringKeys

  protected[this] class ColumnImpl[A](val name: String)(implicit val dataType: DataType[A]) extends Column[A] {
    require(name != null && name != "" && name.trim == name && dataType != null)
    val table = TableLike.this
    _columns += name -> this

    def this(other: Column[A]) {
      this(other.name)(other.dataType)
    }
  }

  protected[this] trait PartitionKeyImpl[A] extends ColumnImpl[A] with PartitionKey[A] {
    _partitionKeys += this
  }

  protected[this] trait ClusteringKeyImpl[A] extends ColumnImpl[A] with ClusteringKey[A] {
    _clusteringKeys += this
  }

  protected[this] val _columns = mutable.LinkedHashMap[String, Column[_]]()
  protected[this] val _partitionKeys = mutable.ArrayBuffer[PartitionKey[_]]()
  protected[this] val _clusteringKeys = mutable.ArrayBuffer[ClusteringKey[_]]()

  protected[this] object PARTITION {
    def KEY(pk: PartitionKey[_]*) {
      require(_partitionKeys.toSet == pk.toSet)
      _partitionKeys.clear()
      _partitionKeys ++= pk
    }
  }

  protected[this] object CLUSTERING {
    def KEY(ck: ClusteringKey[_]*) {
      require(_clusteringKeys.toSet == ck.toSet)
      _clusteringKeys.clear()
      _clusteringKeys ++= ck
    }
  }

  protected[this] object PRIMARY {
    def KEY(pkp: Product, ck: ClusteringKey[_]*) {
      val pk = pkp.productIterator.toStream.map(_.asInstanceOf[PartitionKey[_]])
      PARTITION KEY (pk: _*)
      CLUSTERING KEY (ck: _*)
    }
  }

  protected[this] trait KeyBuilder[B[_] <: PrimaryKey[_]] {
    def apply[A](c: Column[A]): B[A]
  }

  protected[this] object PK extends KeyBuilder[PartitionKey] {
    def apply[A](c: Column[A]) = new ColumnImpl(c) with PartitionKeyImpl[A]
  }

  protected[this] object CK extends KeyBuilder[ClusteringKey] {
    def apply[A](c: Column[A]) = new ColumnImpl(c) with ClusteringKeyImpl[A]
  }

  protected[this] implicit def buildCol[A: DataType](name: String): Column[A] = new ColumnImpl[A](name)

  protected[this] implicit class ColumnBuilder(val columnName: String) {

    def column[A](tp: DataType[A]) = buildCol(columnName)(tp)

    def INT = column(org.cql3scala.INT)
    def BIGINT = column(org.cql3scala.BIGINT)
    def TEXT = column(org.cql3scala.TEXT)
    def BOOLEAN = column(org.cql3scala.BOOLEAN)
    def BLOB = column(org.cql3scala.BLOB)
    def COUNTER = new ColumnImpl(columnName)(org.cql3scala.COUNTER) with CounterColumn
    def TIMESTAMP = column(org.cql3scala.TIMESTAMP)
    def TIMEUUID = column(org.cql3scala.TIMEUUID)
    def FLOAT = column(org.cql3scala.FLOAT)
    def DOUBLE = column(org.cql3scala.DOUBLE)
    def DECIMAL = column(org.cql3scala.DECIMAL)
    def VARINT = column(org.cql3scala.VARINT)

    def INT[B[_] <: PrimaryKey[_]](toKey: KeyBuilder[B]): B[Int] = toKey(INT)
    def BIGINT[B[_] <: PrimaryKey[_]](toKey: KeyBuilder[B]): B[Long] = toKey(BIGINT)
    def TEXT[B[_] <: PrimaryKey[_]](toKey: KeyBuilder[B]): B[String] = toKey(TEXT)
    def BLOB[B[_] <: PrimaryKey[_]](toKey: KeyBuilder[B]): B[Array[Byte]] = toKey(BLOB)
    def TIMESTAMP[B[_] <: PrimaryKey[_]](toKey: KeyBuilder[B]): B[java.util.Date] = toKey(TIMESTAMP)
    def TIMEUUID[B[_] <: PrimaryKey[_]](toKey: KeyBuilder[B]): B[java.util.UUID] = toKey(TIMEUUID)
    def FLOAT[B[_] <: PrimaryKey[_]](toKey: KeyBuilder[B]): B[Float] = toKey(FLOAT)
    def DOUBLE[B[_] <: PrimaryKey[_]](toKey: KeyBuilder[B]): B[Double] = toKey(DOUBLE)
    def DECIMAL[B[_] <: PrimaryKey[_]](toKey: KeyBuilder[B]): B[java.math.BigDecimal] = toKey(DECIMAL)
    def VARINT[B[_] <: PrimaryKey[_]](toKey: KeyBuilder[B]): B[java.math.BigInteger] = toKey(VARINT)

    def SET[A](eType: ElemType[A]) =
      new ColumnImpl(columnName)(org.cql3scala.collection(org.cql3scala.SET, eType)) with SetColumn[A] //column(DataTypes.collection(DataTypes.SET, eType))
    def SET[A <: AnyVal, B](eType: PrimitiveDataType[A, B]) =
      new ColumnImpl(columnName)(org.cql3scala.primitiveCollection(org.cql3scala.SET, eType)) with SetColumn[A] //column(DataTypes.primitiveCollection(DataTypes.SET, eType))
    //TODO add specific operation traits for list and map
    def LIST[A](eType: ElemType[A]) =
      column(org.cql3scala.collection(org.cql3scala.LIST, eType))
    def LIST[A <: AnyVal, B](eType: PrimitiveDataType[A, B]) =
      column(org.cql3scala.primitiveCollection(org.cql3scala.LIST, eType))
    def MAP[K, V](kType: ElemType[K], vType: ElemType[V]) = column(org.cql3scala.map(kType, vType))

  }

  protected[this] def STATIC[A](c: Column[A]): Column[A] with StaticColumn =
    new ColumnImpl(c) with StaticColumn

  protected[this] def WITH(ops: TableOption*) {
    options ++= ops
  }

  protected[this] val options = mutable.ListBuffer[TableOption]()

}

object Table {
  implicit def table2Name(t: Table) = t.name
}

abstract class Table(val name: String, val keyspace: String) extends Equals with TableLike {

  lazy val queryStringForPreparedInsert = "INSERT INTO " + name +
    columnNames.mkString(" (", ",", ")") + Seq.fill(columns.size)("?").mkString(" VALUES (", ",", ");")

  def ddl = {
    assume(!partitionKeys.isEmpty, "Table must contain at least one partition key")
    s"CREATE TABLE $name (" +
      columns.map(_.ddl).mkString("", ", ", ", ") +
      "PRIMARY KEY (" + partitionKeys.map(_.name).mkString("(", ", ", ")") +
      (if (clusteringKeys.isEmpty) "" else clusteringKeys.map(_.name).mkString(", ", ", ", "")) +
      "))" +
      (if (options.isEmpty) "" else " WITH " + options.map(_.ddl).mkString(" AND ")) + ";"
  }

  override def toString = name

  def canEqual(other: Any) = {
    other.isInstanceOf[Table]
  }

  override def equals(other: Any) = {
    other match {
      case that: Table => that.canEqual(Table.this) && keyspace == that.keyspace && name == that.name
      case _ => false
    }
  }

  override def hashCode() = {
    val prime = 41
    prime * (prime + keyspace.hashCode) + name.hashCode
  }

}
