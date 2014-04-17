package org.cql3scala

import java.util.Date
import java.util.UUID
import com.datastax.driver.core.Row
import org.cql3_scala.utils.ByteBufferUtil
import com.datastax.driver.core.querybuilder.QueryBuilder
import java.nio.ByteBuffer
import scala.collection.JavaConversions._
import com.datastax.driver.core.BoundStatement

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
  def qbGt(col: Column[A], value: B) = QueryBuilder.gt(col.name, value)
  def qbGte(col: Column[A], value: B) = QueryBuilder.gte(col.name, value)
  def qbLt(col: Column[A], value: B) = QueryBuilder.lt(col.name, value)
  def qbLte(col: Column[A], value: B) = QueryBuilder.lte(col.name, value)
  def qbIn(col: Column[A], value: Seq[B]) = QueryBuilder.in(col.name, value.asInstanceOf[Seq[Object]]: _*)
  def qbSet(col: Column[A], value: B) = QueryBuilder.set(col.name, value)
  def bind(col: Column[A], b: BoundStatement, value: B)
  def associate(name: String, value: B): (String, Any) = (name, value)
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
  def bind[E](col: Column[C[E]], b: BoundStatement, value: C[E])
}

class CollectionDataType[E, C[_]](implicit val containerType: ContainerType[C], val elemType: ElemType[E])
  extends DataType[C[E]](s"""$containerType<$elemType>""") {
  val cls = containerType.getCls[E]
  def get(col: Column[C[E]], row: Row) = containerType.get(col, row)
  def bind(col: Column[C[E]], b: BoundStatement, value: C[E]) { containerType.bind(col, b, value) }
}

class PrimitiveCollectionDataType[E <: AnyVal, B, C[_]](implicit containerType: ContainerType[C], elemType: PrimitiveDataType[E, B])
  extends CollectionDataType[E, C] {
  override def get(col: Column[C[E]], row: Row) = containerType.getPrimitive(col, row)
}

class MapDataType[K, V](implicit val kType: ElemType[K], val vType: ElemType[V])
  extends DataType[DataTypes.Map[K, V]](s"""map<$kType,$vType>""") {

  val cls = classOf[DataTypes.Map[K, V]]

  def get(col: Column[DataTypes.Map[K, V]], row: Row) = {
    row.getMap(col.name, kcls, vcls).asInstanceOf[DataTypes.Map[K, V]]
  }

  def bind(col: Column[DataTypes.Map[K, V]], b: BoundStatement, value: DataTypes.Map[K, V]) {
    b.setMap(col.name, value)
  }

  private[this] val kcls = getCls(kType)
  private[this] val vcls = getCls(vType)

  private[this] def getCls(t: DataType[_]) = t match {
    case p: PrimitiveDataType[_, _] => p.boxCls
    case r => r.cls
  }

}

object DataTypes {

  type Set[T] = java.util.Set[T]

  type List[T] = java.util.List[T]

  type Map[K, V] = java.util.Map[K, V]

  implicit object SET extends ContainerType[Set]("set") {
    def getCls[E] = classOf[Set[E]]
    def get[E](col: Column[Set[E]], row: Row)(implicit ev: DataType[E]) = row.getSet(col.name, ev.cls)
    def getPrimitive[E <: AnyVal, B](col: Column[Set[E]], row: Row)(implicit ev: PrimitiveDataType[E, B]): Set[E] =
      row.getSet(col.name, ev.boxCls).asInstanceOf[java.util.Set[E]]
    def bind[E](col: Column[Set[E]], b: BoundStatement, value: Set[E]) {
      b.setSet(col.name, value)
    }
  }

  implicit object LIST extends ContainerType[List]("list") {
    def getCls[E] = classOf[List[E]]
    def get[E](col: Column[List[E]], row: Row)(implicit ev: DataType[E]) = row.getList(col.name, ev.cls)
    def getPrimitive[E <: AnyVal, B](col: Column[List[E]], row: Row)(implicit ev: PrimitiveDataType[E, B]): List[E] =
      row.getList(col.name, ev.boxCls).asInstanceOf[java.util.List[E]]
    def bind[E](col: Column[List[E]], b: BoundStatement, value: List[E]) {
      b.setList(col.name, value)
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
    def bind(col: Column[Int], b: BoundStatement, value: Int) {
      b.setInt(col.name, value)
    }
  }

  implicit object BIGINT extends PrimitiveDataType[Long, java.lang.Long]("bigint") {
    val cls = classOf[Long]
    val boxCls = classOf[java.lang.Long]
    def get(col: Column[Long], row: Row) = row.getLong(col.name)
    def bind(col: Column[Long], b: BoundStatement, value: Long) {
      b.setLong(col.name, value)
    }
  }

  implicit object TEXT extends DataType[String]("text") {
    val cls = classOf[String]
    def get(col: Column[String], row: Row) = row.getString(col.name)
    def bind(col: Column[String], b: BoundStatement, value: String) {
      b.setString(col.name, value)
    }
  }

  implicit object BOOLEAN extends PrimitiveDataType[Boolean, java.lang.Boolean]("boolean") {
    val cls = classOf[Boolean]
    val boxCls = classOf[java.lang.Boolean]
    def get(col: Column[Boolean], row: Row) = row.getBool(col.name)
    def bind(col: Column[Boolean], b: BoundStatement, value: Boolean) {
      b.setBool(col.name, value)
    }
  }

  implicit object BLOB extends DataType[Array[Byte]]("blob") {
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
    def bind(col: Column[Array[Byte]], b: BoundStatement, value: Array[Byte]) {
      b.setBytesUnsafe(col.name, c(value))
    }
    override def associate(name: String, value: Array[Byte]) = (name, c(value))
    private def c(value: Array[Byte]) = ByteBuffer.wrap(value)
  }

  implicit object BlobAsByteBuffer extends DataOps[Array[Byte], ByteBuffer] {
    def get(col: Column[Array[Byte]], row: Row) = row.getBytesUnsafe(col.name)
    def bind(col: Column[Array[Byte]], b: BoundStatement, value: ByteBuffer) {
      b.setBytesUnsafe(col.name, value)
    }
  }

  implicit object TIMESTAMP extends DataType[Date]("timestamp") {
    val cls = classOf[Date]
    def get(col: Column[Date], row: Row) = row.getDate(col.name)
    def bind(col: Column[Date], b: BoundStatement, value: Date) {
      b.setDate(col.name, value)
    }
  }

  implicit object TIMEUUID extends DataType[UUID]("timeuuid") {
    val cls = classOf[UUID]
    def get(col: Column[UUID], row: Row) = row.getUUID(col.name)
    def bind(col: Column[UUID], b: BoundStatement, value: UUID) {
      b.setUUID(col.name, value)
    }
  }

  implicit object FLOAT extends PrimitiveDataType[Float, java.lang.Float]("float") {
    val cls = classOf[Float]
    val boxCls = classOf[java.lang.Float]
    def get(col: Column[Float], row: Row) = row.getFloat(col.name)
    def bind(col: Column[Float], b: BoundStatement, value: Float) {
      b.setFloat(col.name, value)
    }
  }

  implicit object DOUBLE extends PrimitiveDataType[Double, java.lang.Double]("double") {
    val cls = classOf[Double]
    val boxCls = classOf[java.lang.Double]
    def get(col: Column[Double], row: Row) = row.getDouble(col.name)
    def bind(col: Column[Double], b: BoundStatement, value: Double) {
      b.setDouble(col.name, value)
    }
  }

  implicit object DECIMAL extends DataType[java.math.BigDecimal]("decimal") {
    val cls = classOf[java.math.BigDecimal]
    def get(col: Column[java.math.BigDecimal], row: Row) = row.getDecimal(col.name)
    def bind(col: Column[java.math.BigDecimal], b: BoundStatement, value: java.math.BigDecimal) {
      b.setDecimal(col.name, value)
    }
  }

  implicit object VARINT extends DataType[java.math.BigInteger]("varint") {
    val cls = classOf[java.math.BigInteger]
    def get(col: Column[java.math.BigInteger], row: Row) = row.getVarint(col.name)
    def bind(col: Column[java.math.BigInteger], b: BoundStatement, value: java.math.BigInteger) {
      b.setVarint(col.name, value)
    }
  }

  object COUNTER extends PrimitiveDataType[Long, java.lang.Long]("counter") {
    val cls = classOf[Long]
    val boxCls = classOf[java.lang.Long]
    def get(col: Column[Long], row: Row) = row.getLong(col.name)
    def bind(col: Column[Long], b: BoundStatement, value: Long) {
      b.setLong(col.name, value)
    }
  }

}