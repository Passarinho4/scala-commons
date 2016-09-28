package com.avsystem.commons
package redis.commands

import akka.util.{ByteString, ByteStringBuilder}
import com.avsystem.commons.misc.{NamedEnum, NamedEnumCompanion, Opt}
import com.avsystem.commons.redis.CommandEncoder.CommandArg
import com.avsystem.commons.redis._
import com.avsystem.commons.redis.exception.UnexpectedReplyException
import com.avsystem.commons.redis.protocol._

import scala.collection.mutable.ArrayBuffer

/**
  * Author: ghik
  * Created: 06/04/16.
  */
trait ClusteredKeysApi extends ApiSubset {
  def del(keys: Seq[Key]): Result[Long] =
    execute(Del(keys))
  def dump(key: Key): Result[Opt[Dumped]] =
    execute(Dump(key))
  def exists(keys: Seq[Key]): Result[Long] =
    execute(Exists(keys))
  def expire(key: Key, seconds: Long): Result[Boolean] =
    execute(Expire(key, seconds))
  def expireat(key: Key, timestamp: Long): Result[Boolean] =
    execute(Expireat(key, timestamp))
  def migrate(keys: Seq[Key], address: NodeAddress, destinationDb: Int,
    timeout: Long, copy: Boolean = false, replace: Boolean = false): Result[Boolean] =
    execute(Migrate(keys, address, destinationDb, timeout, copy, replace))

  def objectRefcount(key: Key): Result[Opt[Long]] =
    execute(ObjectRefcount(key))
  def objectEncoding(key: Key): Result[Opt[Encoding]] =
    execute(ObjectEncoding(key))
  def objectIdletime(key: Key): Result[Opt[Long]] =
    execute(ObjectIdletime(key))

  def persist(key: Key): Result[Boolean] =
    execute(Persist(key))
  def pexpire(key: Key, milliseconds: Long): Result[Boolean] =
    execute(Pexpire(key, milliseconds))
  def pexpireat(key: Key, millisecondsTimestamp: Long): Result[Boolean] =
    execute(Pexpireat(key, millisecondsTimestamp))
  def pttl(key: Key): Result[Opt[Opt[Long]]] =
    execute(Pttl(key))

  def rename(key: Key, newkey: Key): Result[Unit] =
    execute(Rename(key, newkey))
  def renamenx(key: Key, newkey: Key): Result[Boolean] =
    execute(Renamenx(key, newkey))
  def restore(key: Key, ttl: Long, dumpedValue: Dumped, replace: Boolean = false): Result[Unit] =
    execute(Restore(key, ttl, dumpedValue, replace))

  def sort(key: Key, by: Opt[SortPattern[Key, HashKey]] = Opt.Empty, limit: Opt[SortLimit] = Opt.Empty,
    sortOrder: SortOrder = SortOrder.Asc, alpha: Boolean = false): Result[Seq[Value]] =
    execute(Sort(key, by, limit, sortOrder, alpha))
  def sortGet(key: Key, gets: Seq[SortPattern[Key, HashKey]], by: Opt[SortPattern[Key, HashKey]] = Opt.Empty, limit: Opt[SortLimit] = Opt.Empty,
    sortOrder: SortOrder = SortOrder.Asc, alpha: Boolean = false): Result[Seq[Seq[Opt[Value]]]] =
    execute(SortGet(key, gets, by, limit, sortOrder, alpha))
  def sortStore(key: Key, destination: Key, by: Opt[SortPattern[Key, HashKey]] = Opt.Empty, limit: Opt[SortLimit] = Opt.Empty,
    gets: Seq[SortPattern[Key, HashKey]] = Nil, sortOrder: SortOrder = SortOrder.Asc, alpha: Boolean = false): Result[Long] =
    execute(SortStore(key, destination, by, limit, gets, sortOrder, alpha))

  def ttl(key: Key): Result[Opt[Opt[Long]]] =
    execute(Ttl(key))
  def `type`(key: Key): Result[RedisType] =
    execute(Type(key))

  private case class Del(keys: Seq[Key]) extends RedisLongCommand with NodeCommand {
    require(keys.nonEmpty, "DEL requires at least one key")
    val encoded = encoder("DEL").keys(keys).result
  }

  private case class Dump(key: Key) extends RedisOptCommand[Dumped] with NodeCommand {
    val encoded = encoder("DUMP").key(key).result
    protected def decodeNonEmpty(bytes: ByteString) = Dumped(bytes)
  }

  private case class Exists(keys: Seq[Key]) extends RedisLongCommand with NodeCommand {
    require(keys.nonEmpty, "EXISTS requires at least one key")
    val encoded = encoder("EXISTS").keys(keys).result
  }

  private case class Expire(key: Key, seconds: Long) extends RedisBooleanCommand with NodeCommand {
    val encoded = encoder("EXPIRE").key(key).add(seconds).result
  }

  private case class Expireat(key: Key, timestamp: Long) extends RedisBooleanCommand with NodeCommand {
    val encoded = encoder("EXPIREAT").key(key).add(timestamp).result
  }

  private case class Migrate(keys: Seq[Key], address: NodeAddress, destinationDb: Int,
    timeout: Long, copy: Boolean, replace: Boolean) extends RedisCommand[Boolean] with NodeCommand {
    require(keys.nonEmpty, "MIGRATE requires at least one key")

    private val multiKey = keys.size > 1

    val encoded = {
      val enc = encoder("MIGRATE").add(address.ip).add(address.port)
      if (multiKey) {
        enc.add(ByteString.empty)
      } else {
        enc.key(keys.head)
      }
      enc.add(destinationDb).add(timeout).addFlag("COPY", copy).addFlag("REPLACE", replace)
      if (multiKey) {
        enc.add("KEYS").keys(keys)
      }
      enc.result
    }

    def decodeExpected = {
      case SimpleStringStr("OK") => true
      case SimpleStringStr("NOKEY") => false
    }
  }

  private case class ObjectRefcount(key: Key) extends RedisOptLongCommand with NodeCommand {
    val encoded = encoder("OBJECT", "REFCOUNT").key(key).result
  }

  private case class ObjectEncoding(key: Key) extends RedisCommand[Opt[Encoding]] with NodeCommand {
    val encoded = encoder("OBJECT", "ENCODING").key(key).result
    def decodeExpected = {
      case BulkStringMsg(string) => Opt(Encoding.byName(string.utf8String))
      case NullBulkStringMsg => Opt.Empty
    }
  }

  private case class ObjectIdletime(key: Key) extends RedisOptLongCommand with NodeCommand {
    val encoded = encoder("OBJECT", "IDLETIME").key(key).result
  }

  private case class Persist(key: Key) extends RedisBooleanCommand with NodeCommand {
    val encoded = encoder("PERSIST").key(key).result
  }

  private case class Pexpire(key: Key, milliseconds: Long) extends RedisBooleanCommand with NodeCommand {
    val encoded = encoder("PEXPIRE").key(key).add(milliseconds).result
  }

  private case class Pexpireat(key: Key, millisecondsTimestamp: Long) extends RedisBooleanCommand with NodeCommand {
    val encoded = encoder("PEXPIREAT").key(key).add(millisecondsTimestamp).result
  }

  private case class Pttl(key: Key) extends RedisCommand[Opt[Opt[Long]]] with NodeCommand {
    val encoded = encoder("PTTL").key(key).result
    def decodeExpected = {
      case IntegerMsg(-2) => Opt.Empty
      case IntegerMsg(-1) => Opt(Opt.Empty)
      case IntegerMsg(ttl) => Opt(Opt(ttl))
    }
  }

  private case class Rename(key: Key, newkey: Key) extends RedisUnitCommand with NodeCommand {
    val encoded = encoder("RENAME").key(key).key(newkey).result
  }

  private case class Renamenx(key: Key, newkey: Key) extends RedisBooleanCommand with NodeCommand {
    val encoded = encoder("RENAMENX").key(key).key(newkey).result
  }

  private case class Restore(key: Key, ttl: Long, dumpedValue: Dumped, replace: Boolean)
    extends RedisUnitCommand with NodeCommand {
    val encoded = encoder("RESTORE").key(key).add(ttl).add(dumpedValue.raw).addFlag("REPLACE", replace).result
  }

  private abstract class AbstractSort[T](key: Key, by: Opt[SortPattern[Key, HashKey]], limit: Opt[SortLimit],
    gets: Seq[SortPattern[Key, HashKey]], sortOrder: SortOrder, alpha: Boolean, destination: Opt[Key]) extends RedisCommand[T] with NodeCommand {
    val encoded = {
      val enc = encoder("SORT").key(key).optAdd("BY", by).optAdd("LIMIT", limit)
      gets.foreach(sp => enc.add("GET").add(sp))
      enc.add(sortOrder).addFlag("ALPHA", alpha).optKey("STORE", destination).result
    }
  }

  private case class Sort(key: Key, by: Opt[SortPattern[Key, HashKey]], limit: Opt[SortLimit], sortOrder: SortOrder, alpha: Boolean)
    extends AbstractSort[Seq[Value]](key, by, limit, Nil, sortOrder, alpha, Opt.Empty) with RedisDataSeqCommand[Value] with HasValueCodec

  private case class SortGet(key: Key, gets: Seq[SortPattern[Key, HashKey]], by: Opt[SortPattern[Key, HashKey]], limit: Opt[SortLimit], sortOrder: SortOrder, alpha: Boolean)
    extends AbstractSort[Seq[Seq[Opt[Value]]]](key, by, limit, gets, sortOrder, alpha, Opt.Empty) {

    def decodeExpected = {
      case ArrayMsg(elements) =>
        val valuesPerKey = gets.size min 1
        val it = elements.iterator.map {
          case NullBulkStringMsg => Opt.Empty
          case BulkStringMsg(bytes) => Opt(valueCodec.read(bytes))
          case msg => throw new UnexpectedReplyException(s"Expected multi bulk reply but one of the elements is $msg")
        }.grouped(valuesPerKey)
        it.to[ArrayBuffer]
    }
  }

  private case class SortStore(key: Key, destination: Key, by: Opt[SortPattern[Key, HashKey]], limit: Opt[SortLimit], gets: Seq[SortPattern[Key, HashKey]], sortOrder: SortOrder, alpha: Boolean)
    extends AbstractSort[Long](key, by, limit, gets, sortOrder, alpha, Opt(destination)) with RedisLongCommand

  private case class Ttl(key: Key) extends RedisCommand[Opt[Opt[Long]]] with NodeCommand {
    val encoded = encoder("TTL").key(key).result
    def decodeExpected = {
      case IntegerMsg(-2) => Opt.Empty
      case IntegerMsg(-1) => Opt(Opt.Empty)
      case IntegerMsg(ttl) => Opt(Opt(ttl))
    }
  }

  private case class Type(key: Key) extends RedisCommand[RedisType] with NodeCommand {
    val encoded = encoder("TYPE").key(key).result
    def decodeExpected = {
      case SimpleStringStr(str) => RedisType.byName(str)
    }
  }
}

trait NodeKeysApi extends ClusteredKeysApi with ApiSubset {
  def move(key: Key, db: Int): Result[Boolean] =
    execute(Move(key, db))
  def keys(pattern: Key): Result[Seq[Key]] =
    execute(Keys(pattern))
  def scan(cursor: Cursor, matchPattern: Opt[Key] = Opt.Empty, count: Opt[Long] = Opt.Empty): Result[(Cursor, Seq[Key])] =
    execute(Scan(cursor, matchPattern, count))
  def randomkey: Result[Opt[Key]] =
    execute(Randomkey)
  def wait(numslaves: Int, timeout: Long): Result[Long] =
    execute(Wait(numslaves, timeout))

  private case class Move(key: Key, db: Int) extends RedisBooleanCommand with NodeCommand {
    val encoded = encoder("MOVE").key(key).add(db).result
  }

  private case class Keys(pattern: Key) extends RedisDataSeqCommand[Key] with HasKeyCodec with NodeCommand {
    val encoded = encoder("KEYS").value(pattern).result
  }

  private case class Scan(cursor: Cursor, matchPattern: Opt[Key], count: Opt[Long])
    extends RedisCommand[(Cursor, Seq[Key])] with NodeCommand {
    val encoded = encoder("SCAN").add(cursor.raw).optValue("MATCH", matchPattern).optAdd("COUNT", count).result
    def decodeExpected = {
      case ArrayMsg(IndexedSeq(BulkStringMsg(cursorString), ArrayMsg(elements))) =>
        (Cursor(cursorString.utf8String.toLong), elements.map {
          case BulkStringMsg(bs) => keyCodec.read(bs)
          case msg => throw new UnexpectedReplyException(s"Expected multi bulk reply, but one of the elements is $msg")
        })
    }
  }

  private case object Randomkey extends RedisOptDataCommand[Key] with HasKeyCodec with NodeCommand {
    val encoded = encoder("RANDOMKEY").result
  }

  private case class Wait(numslaves: Int, timeout: Long) extends RedisLongCommand with NodeCommand {
    val encoded = encoder("WAIT").add(numslaves).add(timeout).result
  }
}

case class Dumped(raw: ByteString) extends AnyVal

sealed abstract class Encoding(val name: String) extends NamedEnum
sealed trait StringEncoding extends Encoding
sealed trait ListEncoding extends Encoding
sealed trait SetEncoding extends Encoding
sealed trait HashEncoding extends Encoding
sealed trait SortedSetEncoding extends Encoding

object Encoding extends NamedEnumCompanion[Encoding] {
  case object Raw extends Encoding("raw") with StringEncoding
  case object Int extends Encoding("int") with StringEncoding
  case object ZipList extends Encoding("ziplist") with ListEncoding with HashEncoding with SortedSetEncoding
  case object LinkedList extends Encoding("linkedlist") with ListEncoding
  case object IntSet extends Encoding("intset") with SetEncoding
  case object HashTable extends Encoding("hashtable") with SetEncoding with HashEncoding
  case object SkipList extends Encoding("skiplist") with SortedSetEncoding
  case object EmbStr extends Encoding("embstr") with StringEncoding

  val values: List[Encoding] = caseObjects
}

case class Cursor(raw: Long) extends AnyVal {
  override def toString = raw.toString
}
object Cursor {
  val NoCursor = Cursor(0)
}

case class SortLimit(offset: Long, count: Long)
object SortLimit {
  implicit val SortLimitArg: CommandArg[SortLimit] =
    CommandArg((ce, sl) => ce.add(sl.offset).add(sl.count))
}

sealed trait SortPattern[+K, +H]
case object SelfPattern extends SortPattern[Nothing, Nothing]
case class KeyPattern[+K](pattern: K) extends SortPattern[K, Nothing]
case class HashFieldPattern[+K, +H](keyPattern: K, fieldPattern: H) extends SortPattern[K, H]
object SortPattern {
  implicit def SortPatternArg[K: RedisDataCodec, H: RedisDataCodec]: CommandArg[SortPattern[K, H]] =
    CommandArg((ce, sp) => ce.add(sp match {
      case SelfPattern => ByteString("#")
      case KeyPattern(pattern) => RedisDataCodec.write(pattern)
      case HashFieldPattern(keyPattern, fieldPattern) =>
        val bsb = new ByteStringBuilder
        bsb.append(RedisDataCodec.write(keyPattern))
        bsb.append(ByteString("->"))
        bsb.append(RedisDataCodec.write(fieldPattern))
        bsb.result()
    }))
}

sealed abstract class RedisType(val name: String) extends NamedEnum
object RedisType extends NamedEnumCompanion[RedisType] {
  case object None extends RedisType("none")
  case object String extends RedisType("string")
  case object List extends RedisType("list")
  case object Set extends RedisType("set")
  case object Zset extends RedisType("zset")
  case object Hash extends RedisType("hash")

  val values: List[RedisType] = caseObjects
}
