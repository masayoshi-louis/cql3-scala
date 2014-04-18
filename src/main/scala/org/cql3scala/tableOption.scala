package org.cql3scala

trait TableOption {

  def ddl: String

}

object TableOption {

  implicit class NativeValue(val str: String) {
    override def toString = str
  }
  
}

object CompactStorage extends TableOption {

  def ddl = "COMPACT STORAGE"

}

object ClusteringOrder {

  def BY(spec: String) = new TableOption {

    def ddl = "CLUSTERING ORDER BY " + s"($spec)"

  }

}

abstract class Property[A](key: String) {

  protected def :=(value: A): TableOption = new TableOption {

    def ddl =
      if (value.isInstanceOf[String]) s"""$key = '$value'"""
      else s"$key = $value"

  }

}

object Property {

  def apply(key: String) = new {

    def :=[A](v: A) = new OpenValueProperty[A](key) := v

  }

}

class OpenValueProperty[A](key: String) extends Property[A](key) {

  override def :=(value: A): TableOption = super.:=(value)

}

object Caching extends Property[String]("caching") {

  val All = this := "all"

  val KeysOnly = this := "keys_only"

  val RowsOnly = this := "rows_only"

  val None = this := "none"

}

object BloomFilterFpChance extends OpenValueProperty[Double]("bloom_filter_fp_chance")

object Comment extends OpenValueProperty[String]("comment")

object Compaction extends OpenValueProperty[TableOption.NativeValue]("compaction")

object Compression extends OpenValueProperty[TableOption.NativeValue]("compression")

//object Compaction extends Property[String]("compaction") {
//
//  val SizeTieredCompactionStrategy = this := "SizeTieredCompactionStrategy"
//
//  val LeveledCompactionStrategy = this := "LeveledCompactionStrategy"
//
//}
//
//object Compression extends Property[String]("compression") {
//
//  val LZ4Compressor = this := "LZ4Compressor"
//
//  val SnappyCompressor = this := "SnappyCompressor"
//
//  val DeflateCompressor = this := "DeflateCompressor"
//
//  val Disabled = this := ""
//
//}

object DclocalReadRepairChance extends OpenValueProperty[Double]("dclocal_read_repair_chance")

object GCGraceSeconds extends OpenValueProperty[Int]("gc_grace_seconds")

object PopulateIOCacheOnFlush extends OpenValueProperty[Boolean]("populate_io_cache_on_flush")

object ReadRepairChance extends OpenValueProperty[Double]("read_repair_chance")

object ReplicateOnWrite extends OpenValueProperty[Boolean]("replicate_on_write")
