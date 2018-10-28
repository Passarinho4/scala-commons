package com.avsystem.commons
package redis

import akka.util.ByteString
import com.avsystem.commons.serialization.GenCodec

/**
  * Typeclass which expresses that values of some type are serializable to binary form (`ByteString`) and deserializable
  * from it in order to use them as keys, hash keys and values in Redis commands.
  *
  * By default, `RedisDataCodec` is provided for simple types like `String`, `ByteString`, `Array[Byte]`,
  * `Boolean`, `Char`, all primitive numeric types and `NamedEnum`s
  * (which have `NamedEnumCompanion`).
  *
  * Also, all types which have an instance of `GenCodec`
  * automatically have an instance of RedisDataCodec.
  */
case class RedisDataCodec[T](read: ByteString => T, write: T => ByteString)
object RedisDataCodec extends LowPriorityRedisDataCodecs {
  def apply[T](implicit rdc: RedisDataCodec[T]): RedisDataCodec[T] = rdc

  def write[T](value: T)(implicit rdc: RedisDataCodec[T]): ByteString = rdc.write(value)
  def read[T](raw: ByteString)(implicit rdc: RedisDataCodec[T]): T = rdc.read(raw)

  implicit val ByteStringCodec: RedisDataCodec[ByteString] = RedisDataCodec(identity, identity)
}
trait LowPriorityRedisDataCodecs { this: RedisDataCodec.type =>
  implicit def fromGenCodec[T: GenCodec]: RedisDataCodec[T] =
    RedisDataCodec(bytes => RedisDataInput.read(bytes), value => RedisDataOutput.write(value))
}
