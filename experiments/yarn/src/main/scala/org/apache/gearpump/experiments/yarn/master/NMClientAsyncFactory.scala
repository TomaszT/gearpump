package org.apache.gearpump.experiments.yarn.master

import org.apache.gearpump.experiments.yarn.NodeManagerCallbackHandler
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.slf4j.Logger

//Kam. Not sure if this is a singleton pattern where you only want one instance ever. In which case maybe you just need
//object NMClientAsyncFactory {
//  val LOG: Logger = LogUtil.getLogger(getClass)
//  def newInstance(containerListener: NodeManagerCallbackHandler, yarnConf: YarnConfiguration): NMClientAsync = {
//    LOG.info("Creating NMClientAsync")
//    val nmClient = new NMClientAsyncImpl(containerListener)
//    nmClient.init(yarnConf)
//    nmClient.start()
//    nmClient
//  }
//}
// Otherwise if you want the type but only one instance of the type you could do something like
//trait NMClientAsyncFactory {
//  def newInstance(containerListener: NodeManagerCallbackHandler, yarnConf: YarnConfiguration): NMClientAsync
//}
//
//object NMClientAsyncFactory {
//  val LOG: Logger = LogUtil.getLogger(getClass)
//  val instance: NMClientAsyncFactory = new NMClientAsyncFactory {
//    override def newInstance(containerListener: NodeManagerCallbackHandler, yarnConf: YarnConfiguration): NMClientAsync = {
//      LOG.info("Creating NMClientAsync")
//      val nmClient = new NMClientAsyncImpl(containerListener)
//      nmClient.init(yarnConf)
//      nmClient.start()
//      nmClient
//    }
//  }
//  def apply(): NMClientAsyncFactory = instance
//}
//object Foo {
//  val foo = NMClientAsyncFactory()
//}

trait NMClientAsyncFactory {
  def newInstance(containerListener: NodeManagerCallbackHandler, yarnConf: YarnConfiguration): NMClientAsync
}

object DefaultNMClientAsyncFactory extends NMClientAsyncFactory {

  val LOG: Logger = LogUtil.getLogger(getClass)

  override def newInstance(containerListener: NodeManagerCallbackHandler, yarnConf: YarnConfiguration): NMClientAsync = {
    LOG.info("Creating NMClientAsync")
    val nmClient = new NMClientAsyncImpl(containerListener)
    nmClient.init(yarnConf)
    nmClient.start()
    nmClient
  }
}

