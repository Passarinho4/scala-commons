package com.avsystem.commons
package redis

import akka.util.ByteString
import com.avsystem.commons.redis.protocol.{ArrayMsg, BulkStringMsg}
import com.avsystem.commons.serialization.GenCodec.ReadFailure
import com.avsystem.commons.serialization.json.{JsonReader, JsonStringInput, JsonStringOutput}
import com.avsystem.commons.serialization.{FieldInput, GenCodec, InputAndSimpleInput, ListInput, ListOutput, ObjectInput, ObjectOutput, Output, OutputAndSimpleOutput}

object RedisDataMarkers {
  final val Null: Byte = 0
  final val Bytes: Byte = 1
  final val Complex: Byte = 2
}

object RedisDataOutput {
  def write[T: GenCodec](value: T): ByteString = {
    var bs: ByteString = null
    GenCodec.write(new RedisDataOutput(bs = _), value)
    bs
  }
}

final class RedisDataOutput(consumer: ByteString => Unit) extends OutputAndSimpleOutput {
  private def marker(marker: Byte): ByteString =
    ByteString(0, marker)

  private def writeBytes(bytes: ByteString): Unit =
    if (bytes.nonEmpty && bytes.head == 0)
      consumer(marker(RedisDataMarkers.Bytes) ++ bytes)
    else
      consumer(bytes)

  def writeNull(): Unit = consumer(marker(RedisDataMarkers.Null))
  def writeBoolean(boolean: Boolean): Unit = writeInt(if (boolean) 1 else 0)
  def writeString(str: String): Unit = writeBytes(ByteString(str))
  def writeInt(int: Int): Unit = writeString(int.toString)
  def writeLong(long: Long): Unit = writeString(long.toString)
  def writeDouble(double: Double): Unit = writeString(double.toString)
  def writeBigInt(bigInt: BigInt): Unit = writeString(bigInt.toString)
  def writeBigDecimal(bigDecimal: BigDecimal): Unit = writeString(bigDecimal.toString)
  def writeBinary(binary: Array[Byte]): Unit = writeBytes(ByteString(binary))

  def writeList(): ListOutput = new ListOutput {
    private val sb = new JStringBuilder
    private val jlo = new JsonStringOutput(sb).writeList()

    def writeElement(): Output = jlo.writeElement()
    def finish(): Unit = {
      jlo.finish()
      consumer(marker(RedisDataMarkers.Complex) ++ ByteString(sb.toString))
    }
  }

  def writeObject(): ObjectOutput = new ObjectOutput {
    private val sb = new JStringBuilder
    private val joo = new JsonStringOutput(sb).writeObject()

    def writeField(key: String): Output = joo.writeField(key)
    def finish(): Unit = {
      joo.finish()
      consumer(marker(RedisDataMarkers.Complex) ++ ByteString(sb.toString))
    }
  }
}

object RedisDataInput {
  def read[T: GenCodec](bytes: ByteString): T =
    GenCodec.read[T](new RedisDataInput(bytes))
}

class RedisDataInput(bytes: ByteString) extends InputAndSimpleInput {
  private val marked = bytes.length >= 2 && bytes.head == 0
  private val marker = if (marked) bytes(1) else 0

  private lazy val jsonInput = new JsonStringInput(new JsonReader(bytes.drop(2).utf8String))

  def isNull: Boolean = marked && marker == RedisDataMarkers.Null
  def isList: Boolean = marked && marker == RedisDataMarkers.Complex && jsonInput.isList
  def isObject: Boolean = marked && marker == RedisDataMarkers.Complex && jsonInput.isObject

  private def fail(msg: String) = throw new ReadFailure(msg)
  private def readBytes(): ByteString =
    if (marked) marker match {
      case RedisDataMarkers.Bytes => bytes.drop(2)
      case RedisDataMarkers.Complex => fail("complex value")
      case RedisDataMarkers.Null => fail("null")
    } else bytes

  def readNull(): Null = if (isNull) null else fail("not null")
  def readString(): String = readBytes().utf8String
  def readBoolean(): Boolean = readString().toInt > 0
  def readInt(): Int = readString().toInt
  def readLong(): Long = readString().toLong
  def readDouble(): Double = readString().toDouble
  def readBigInt(): BigInt = BigInt(readString())
  def readBigDecimal(): BigDecimal = BigDecimal(readString())
  def readBinary(): Array[Byte] = readBytes().toArray

  def readList(): ListInput = jsonInput.readList()
  def readObject(): ObjectInput = jsonInput.readObject()

  def skip(): Unit = ()
}

final class RedisRecordObjectOutput extends ObjectOutput {
  private val buffer = new MArrayBuffer[BulkStringMsg]

  def writeField(key: String): Output = {
    buffer += BulkStringMsg(ByteString(key))
    new RedisDataOutput(bs => buffer += BulkStringMsg(bs))
  }

  def finish(): Unit = ()
  def result: ArrayMsg[BulkStringMsg] = ArrayMsg(buffer)
}

final class RedisRecordObjectInput(msg: ArrayMsg[BulkStringMsg]) extends ObjectInput {
  private val it = msg.elements.iterator.map(_.string)

  def nextField(): FieldInput = {
    val fieldName = it.next().utf8String
    new RedisRecordFieldInput(fieldName, it.next())
  }

  def hasNext: Boolean = it.hasNext
}

final class RedisRecordFieldInput(val fieldName: String, bytes: ByteString)
  extends RedisDataInput(bytes) with FieldInput
