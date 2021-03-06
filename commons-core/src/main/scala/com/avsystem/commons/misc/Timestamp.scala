package com.avsystem.commons
package misc

import java.util.concurrent.TimeUnit

import com.avsystem.commons.serialization.{GenCodec, GenKeyCodec, IsoInstant}

import scala.concurrent.duration.{FiniteDuration, TimeUnit}

/**
  * Millisecond-precision, general purpose, cross compiled timestamp representation.
  *
  * @param millis milliseconds since UNIX epoch, UTC
  */
class Timestamp(val millis: Long) extends AnyVal with Comparable[Timestamp] {
  def compareTo(o: Timestamp): Int = java.lang.Long.compare(millis, o.millis)

  // I don't want to inherit them from Ordered or something because that would cause boxing
  def <(other: Timestamp): Boolean = millis < other.millis
  def <=(other: Timestamp): Boolean = millis <= other.millis
  def >(other: Timestamp): Boolean = millis > other.millis
  def >=(other: Timestamp): Boolean = millis >= other.millis

  def add(amount: Long, unit: TimeUnit): Timestamp = Timestamp(millis + unit.toMillis(amount))

  def +(duration: FiniteDuration): Timestamp = Timestamp(millis + duration.toMillis)
  def -(duration: FiniteDuration): Timestamp = Timestamp(millis - duration.toMillis)

  def until(other: Timestamp): FiniteDuration =
    FiniteDuration(other.millis - millis, TimeUnit.MILLISECONDS)

  override def toString: String = IsoInstant.format(millis)
}
object Timestamp {
  final val Zero = Timestamp(0)

  def apply(millis: Long): Timestamp = new Timestamp(millis)
  def unapply(timestamp: Timestamp): Opt[Long] = Opt(timestamp.millis)
  def parse(str: String): Timestamp = Timestamp(IsoInstant.parse(str))

  def now(): Timestamp = Timestamp(System.currentTimeMillis())

  implicit def conversions(tstamp: Timestamp): TimestampConversions =
    new TimestampConversions(tstamp.millis)

  implicit val keyCodec: GenKeyCodec[Timestamp] =
    GenKeyCodec.create(parse, _.toString)

  implicit val codec: GenCodec[Timestamp] =
    GenCodec.nonNullSimple(i => Timestamp(i.readTimestamp()), (o, t) => o.writeTimestamp(t.millis))

  implicit val ordering: Ordering[Timestamp] =
    Ordering.by(_.millis)
}
