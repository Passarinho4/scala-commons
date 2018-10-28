package com.avsystem.commons
package redis

import com.avsystem.commons.redis.protocol.{ArrayMsg, BulkStringMsg}
import com.avsystem.commons.serialization.GenObjectCodec

import scala.collection.generic.CanBuildFrom

case class RedisRecordCodec[T](read: ArrayMsg[BulkStringMsg] => T, write: T => ArrayMsg[BulkStringMsg])
object RedisRecordCodec extends LowPriorityRedisRecordCodecs {
  def apply[T](implicit codec: RedisRecordCodec[T]): RedisRecordCodec[T] = codec

  implicit def forDataMap[M[X, Y] <: BMap[X, Y], F: RedisDataCodec, V: RedisDataCodec](
    implicit cbf: CanBuildFrom[Nothing, (F, V), M[F, V]]
  ): RedisRecordCodec[M[F, V] with BMap[F, V]] = RedisRecordCodec(
    msg => {
      val builder = cbf()
      msg.elements.iterator.grouped(2).foreach {
        case Seq(BulkStringMsg(f), BulkStringMsg(v)) =>
          builder += (RedisDataCodec[F].read(f) -> RedisDataCodec[V].read(v))
      }
      builder.result()
    },
    map => ArrayMsg(map.iterator
      .flatMap({ case (f, v) => List(RedisDataCodec.write(f), RedisDataCodec.write(v)) })
      .map(BulkStringMsg).to[MArrayBuffer]
    )
  )
}
trait LowPriorityRedisRecordCodecs { this: RedisRecordCodec.type =>
  implicit def fromGenObjectCodec[T: GenObjectCodec]: RedisRecordCodec[T] =
    RedisRecordCodec(
      multiBulk => GenObjectCodec.readObject[T](new RedisRecordObjectInput(multiBulk)),
      value => new RedisRecordObjectOutput().setup(GenObjectCodec.writeObject(_, value)).result
    )
}
