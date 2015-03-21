package org.apache.gearpump.experiments.yarn

import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.apache.hadoop.yarn.api.records.ContainerId
import java.nio.ByteBuffer
import org.apache.hadoop.yarn.api.records.ContainerStatus
import org.slf4j.Logger
import org.apache.gearpump.util.LogUtil



class NodeManagerCallbackHandler() extends NMClientAsync.CallbackHandler {
  val LOG = LogUtil.getLogger(getClass)

  def onContainerStarted(containerId: ContainerId, allServiceResponse: java.util.Map[String, ByteBuffer]) {
    LOG.info(s"Container started : $containerId, " + allServiceResponse)
  }
  
  def onContainerStatusReceived(containerId: ContainerId, containerStatus: ContainerStatus) {
    LOG.info(s"Container status received : $containerId, status $containerStatus")
  }

  def onContainerStopped(containerId: ContainerId) {
    LOG.info(s"Container stopped : $containerId")
  }

  def onGetContainerStatusError(containerId: ContainerId, throwable: Throwable) {
    LOG.error(s"Container exception : $containerId", throwable)
  }

  def onStartContainerError(containerId: ContainerId, throwable: Throwable) {
    LOG.error(s"Container exception : $containerId", throwable)
  }

  def onStopContainerError(containerId: ContainerId, throwable: Throwable) {
    LOG.error(s"Container exception : $containerId", throwable)
  }
}

object NodeManagerCallbackHandler {
  def apply() = new NodeManagerCallbackHandler()
}
