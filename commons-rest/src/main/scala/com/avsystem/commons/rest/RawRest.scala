package com.avsystem.commons
package rest

import com.avsystem.commons.concurrent.Async
import com.avsystem.commons.meta._
import com.avsystem.commons.rpc._
import com.avsystem.commons.serialization.GenCodec.ReadFailure

sealed abstract class RestMethodCall {
  val rpcName: String
  val pathParams: List[PathValue]
  val metadata: RestMethodMetadata[_]
}
case class PrefixCall(rpcName: String, pathParams: List[PathValue], metadata: PrefixMetadata[_]) extends RestMethodCall
case class HttpCall(rpcName: String, pathParams: List[PathValue], metadata: HttpMethodMetadata[_]) extends RestMethodCall

case class ResolvedCall(root: RestMetadata[_], prefixes: List[PrefixCall], finalCall: HttpCall) {
  lazy val pathPattern: List[PathPatternElement] =
    if (prefixes.isEmpty) finalCall.metadata.pathPattern
    else (prefixes.iterator.flatMap(_.metadata.pathPattern.iterator) ++
      finalCall.metadata.pathPattern.iterator).toList

  def method: HttpMethod = finalCall.metadata.method

  def rpcChainRepr: String =
    prefixes.iterator.map(_.rpcName).mkString("", "->", s"->${finalCall.rpcName}")
}

@methodTag[RestMethodTag]
trait RawRest {

  import RawRest._

  // declaration order of raw methods matters - it determines their priority!

  @multi @tried
  @tagged[Prefix](whenUntagged = new Prefix)
  @paramTag[RestParamTag](defaultTag = new Path)
  def prefix(@methodName name: String, @composite parameters: RestParameters): Try[RawRest]

  @multi @tried
  @tagged[GET]
  @paramTag[RestParamTag](defaultTag = new Query)
  def get(@methodName name: String, @composite parameters: RestParameters): Async[RestResponse]

  @multi @tried @annotated[FormBody]
  @tagged[BodyMethodTag](whenUntagged = new POST)
  @paramTag[RestParamTag](defaultTag = new BodyField)
  def handleForm(@methodName name: String, @composite parameters: RestParameters,
    @multi @tagged[BodyField] body: Mapping[QueryValue]): Async[RestResponse]

  @multi @tried
  @tagged[BodyMethodTag](whenUntagged = new POST)
  @paramTag[RestParamTag](defaultTag = new BodyField)
  def handle(@methodName name: String, @composite parameters: RestParameters,
    @multi @tagged[BodyField] body: Mapping[JsonValue]): Async[RestResponse]

  @multi @tried
  @tagged[BodyMethodTag](whenUntagged = new POST)
  @paramTag[RestParamTag]
  def handleSingle(@methodName name: String, @composite parameters: RestParameters,
    @encoded @tagged[Body] body: HttpBody): Async[RestResponse]

  def asHandleRequest(metadata: RestMetadata[_]): HandleRequest =
    RawRest.resolveAndHandle(metadata)(handleResolved)

  def handleResolved(request: RestRequest, resolved: ResolvedCall): Async[RestResponse] = {
    val RestRequest(method, parameters, body) = request
    val ResolvedCall(_, prefixes, finalCall) = resolved
    val HttpCall(finalRpcName, finalPathParams, finalMetadata) = finalCall

    def handleBadBody[T](expr: => T): T = try expr catch {
      case rf: ReadFailure => throw new InvalidRpcCall(s"Invalid HTTP body: ${rf.getMessage}", rf)
    }

    def resolveCall(rawRest: RawRest, prefixes: List[PrefixCall]): Async[RestResponse] = prefixes match {
      case PrefixCall(rpcName, pathParams, _) :: tail =>
        rawRest.prefix(rpcName, parameters.copy(path = pathParams)) match {
          case Success(nextRawRest) => resolveCall(nextRawRest, tail)
          case Failure(e: HttpErrorException) => Async.successful(e.toResponse)
          case Failure(cause) => Async.failed(cause)
        }
      case Nil =>
        val finalParameters = parameters.copy(path = finalPathParams)
        if (method == HttpMethod.GET)
          rawRest.get(finalRpcName, finalParameters)
        else if (finalMetadata.singleBody)
          rawRest.handleSingle(finalRpcName, finalParameters, body)
        else if (finalMetadata.formBody)
          rawRest.handleForm(finalRpcName, finalParameters, handleBadBody(HttpBody.parseFormBody(body)))
        else
          rawRest.handle(finalRpcName, finalParameters, handleBadBody(HttpBody.parseJsonBody(body)))
    }
    try resolveCall(this, prefixes) catch {
      case e: InvalidRpcCall =>
        Async.successful(RestResponse.plain(400, e.getMessage))
    }
  }
}

object RawRest extends RawRpcCompanion[RawRest] {
  /**
    * Raw type of an operation that executes a [[RestRequest]]. The operation should be run every time the
    * resulting `Async` value is passed a callback. It should not be run before that. Each run may involve side
    * effects, network communication, etc. Runs may be concurrent.
    * Request handlers should never throw exceptions but rather convert them into failing implementation of
    * `Async`. One way to do this is by wrapping the handler with [[safeHandle]].
    */
  type HandleRequest = RestRequest => Async[RestResponse]

  /**
    * Similar to [[HandleRequest]] but accepts already resolved path as a second argument.
    */
  type HandleResolvedRequest = (RestRequest, ResolvedCall) => Async[RestResponse]

  /**
    * Ensures that all possible exceptions thrown by a request handler are not propagated but converted into
    * an instance of `Async` that notifies its callbacks about the failure.
    */
  def safeHandle(handleRequest: HandleRequest): HandleRequest =
    request => Async.safe(handleRequest(request))

  def fromHandleRequest[Real: AsRealRpc : RestMetadata](handleRequest: HandleRequest): Real =
    RawRest.asReal(new DefaultRawRest(RestMetadata[Real], RestParameters.Empty, handleRequest))

  def asHandleRequest[Real: AsRawRpc : RestMetadata](real: Real): HandleRequest =
    RawRest.asRaw(real).asHandleRequest(RestMetadata[Real])

  def resolveAndHandle(metadata: RestMetadata[_])(handleResolved: HandleResolvedRequest): HandleRequest = {
    metadata.ensureValid()

    RawRest.safeHandle { request =>
      val path = request.parameters.path
      metadata.resolvePath(path) match {
        case Nil =>
          val message = s"path ${PathValue.encodeJoin(path)} not found"
          Async.successful(RestResponse.plain(404, message))
        case calls => request.method match {
          case HttpMethod.OPTIONS =>
            val meths = calls.iterator.map(_.method).flatMap {
              case HttpMethod.GET => List(HttpMethod.GET, HttpMethod.HEAD)
              case m => List(m)
            } ++ Iterator(HttpMethod.OPTIONS)
            val response = RestResponse(200, Mapping("Allow" -> HeaderValue(meths.mkString(","))), HttpBody.Empty)
            Async.successful(response)
          case wireMethod =>
            val head = wireMethod == HttpMethod.HEAD
            val req = if (head) request.copy(method = HttpMethod.GET) else request
            calls.find(_.method == req.method) match {
              case Some(call) =>
                val resp = handleResolved(req, call)
                if (head) Async.map(resp)(_.copy(body = HttpBody.empty)) else resp
              case None =>
                val message = s"$wireMethod not allowed on path ${PathValue.encodeJoin(path)}"
                Async.successful(RestResponse.plain(405, message))
            }
        }
      }
    }
  }

  private final class DefaultRawRest(metadata: RestMetadata[_], prefixHeaders: RestParameters, handleRequest: HandleRequest)
    extends RawRest {

    def prefix(name: String, parameters: RestParameters): Try[RawRest] =
      metadata.prefixMethods.get(name).map { prefixMeta =>
        val newHeaders = prefixHeaders.append(prefixMeta, parameters)
        Success(new DefaultRawRest(prefixMeta.result.value, newHeaders, handleRequest))
      } getOrElse Failure(new RestException(s"no such prefix method: $name"))

    def get(name: String, parameters: RestParameters): Async[RestResponse] =
      handleSingle(name, parameters, HttpBody.Empty)

    def handle(name: String, parameters: RestParameters, body: Mapping[JsonValue]): Async[RestResponse] =
      handleSingle(name, parameters, HttpBody.createJsonBody(body))

    def handleForm(name: String, parameters: RestParameters, body: Mapping[QueryValue]): Async[RestResponse] =
      handleSingle(name, parameters, HttpBody.createFormBody(body))

    def handleSingle(name: String, parameters: RestParameters, body: HttpBody): Async[RestResponse] =
      metadata.httpMethods.get(name).map { methodMeta =>
        val newHeaders = prefixHeaders.append(methodMeta, parameters)
        handleRequest(RestRequest(methodMeta.method, newHeaders, body))
      } getOrElse Async.failed(new RestException(s"no such HTTP method: $name"))
  }
}

class RestException(msg: String, cause: Throwable = null) extends InvalidRpcCall(msg, cause)
