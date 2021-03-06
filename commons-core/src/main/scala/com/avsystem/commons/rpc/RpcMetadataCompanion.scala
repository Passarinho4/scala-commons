package com.avsystem.commons
package rpc

import com.avsystem.commons.macros.rpc.RpcMacros
import com.avsystem.commons.meta.MetadataCompanion

trait RpcMetadataCompanion[M[_]] extends MetadataCompanion[M] {
  def materialize[Real]: M[Real] = macro RpcMacros.rpcMetadata[Real]
}
