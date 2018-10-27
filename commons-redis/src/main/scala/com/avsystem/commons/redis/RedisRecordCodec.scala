package com.avsystem.commons
package redis

import java.io.{DataInputStream, DataOutputStream}

import akka.util.{ByteString, ByteStringBuilder}
import com.avsystem.commons.redis.protocol.{ArrayMsg, BulkStringMsg}
import com.avsystem.commons.serialization.GenCodec.ReadFailure
import com.avsystem.commons.serialization.{FieldInput, GenObjectCodec, InputAndSimpleInput, IsoInstant, ListInput, ListOutput, ObjectInput, ObjectOutput, Output, OutputAndSimpleOutput, StreamInput, StreamOutput}

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

final class RedisRecordObjectOutput extends ObjectOutput {
  private val buffer = new MArrayBuffer[BulkStringMsg]

  def writeField(key: String): Output = {
    buffer += BulkStringMsg(ByteString(key))
    new RedisRecordFieldOutput(buffer)
  }

  def finish(): Unit = ()
  def result: ArrayMsg[BulkStringMsg] = ArrayMsg(buffer)
}

final class RedisRecordFieldOutput(buffer: MArrayBuffer[BulkStringMsg]) extends OutputAndSimpleOutput {
  private def add(bs: ByteString): Unit = {
    val escapedBytes = if (bs.isEmpty || bs.head != 0) bs else ByteString(0) ++ bs
    buffer += BulkStringMsg(escapedBytes)
  }
  private def add(str: String): Unit = add(ByteString(str))

  def writeNull(): Unit = buffer += BulkStringMsg(ByteString(0))
  def writeBoolean(boolean: Boolean): Unit = add(if (boolean) "1" else "0")
  def writeString(str: String): Unit = add(str)
  def writeInt(int: Int): Unit = add(int.toString)
  def writeLong(long: Long): Unit = add(long.toString)
  def writeDouble(double: Double): Unit = add(double.toString)
  def writeBigInt(bigInt: BigInt): Unit = add(bigInt.toString)
  def writeBigDecimal(bigDecimal: BigDecimal): Unit = add(bigDecimal.toString)
  def writeBinary(binary: Array[Byte]): Unit = add(ByteString(binary))
  override def writeTimestamp(millis: Long): Unit = add(IsoInstant.format(millis))

  def writeList(): ListOutput = new ListOutput {
    private val bsb = new ByteStringBuilder
    private val slo = new StreamOutput(new DataOutputStream(bsb.asOutputStream)).writeList()

    def writeElement(): Output = slo.writeElement()
    def finish(): Unit = add(bsb.result())
  }

  def writeObject(): ObjectOutput = new ObjectOutput {
    private val bsb = new ByteStringBuilder
    private val soo = new StreamOutput(new DataOutputStream(bsb.asOutputStream)).writeObject()

    def writeField(key: String): Output = soo.writeField(key)
    def finish(): Unit = add(bsb.result())
  }
}

final class RedisRecordObjectInput(msg: ArrayMsg[BulkStringMsg]) extends ObjectInput {
  private val it = msg.elements.iterator.map(_.string)

  def nextField(): FieldInput = new RedisRecordFieldInput(it)
  def hasNext: Boolean = it.hasNext
}

final class RedisRecordFieldInput(it: Iterator[ByteString]) extends InputAndSimpleInput with FieldInput {
  val fieldName: String = it.next().utf8String

  private val valueBytes: ByteString = it.next() match {
    case bs if bs.nonEmpty && bs(0) == 0 =>
      if (bs.length > 1) bs.drop(1) else null
    case bs => bs
  }
  private lazy val streamInput =
    new StreamInput(new DataInputStream(valueBytes.iterator.asInputStream))

  def isNull: Boolean = valueBytes == null
  def isList: Boolean = valueBytes.nonEmpty && streamInput.isList
  def isObject: Boolean = valueBytes.nonEmpty && streamInput.isObject

  private def fail(msg: String) = throw new ReadFailure(msg)
  private def bytes: ByteString = if (valueBytes != null) valueBytes else fail("null")
  private def str: String = bytes.utf8String

  def readNull(): Null = if (isNull) null else fail("not null")
  def readBoolean(): Boolean = str.toInt > 0
  def readString(): String = str
  def readInt(): Int = str.toInt
  def readLong(): Long = str.toLong
  def readDouble(): Double = str.toDouble
  def readBigInt(): BigInt = BigInt(str)
  def readBigDecimal(): BigDecimal = BigDecimal(str)
  def readBinary(): Array[Byte] = bytes.toArray
  override def readTimestamp(): Long = IsoInstant.parse(str)

  def readList(): ListInput = streamInput.readList()
  def readObject(): ObjectInput = streamInput.readObject()

  def skip(): Unit = ()
}
