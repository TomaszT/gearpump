package org.apache.gearpump.experiments.yarn.master

import akka.actor._
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.yarn.api.records.{Container, ContainerLaunchContext}
import org.apache.hadoop.yarn.client.api.async.NMClientAsync


class ContainerLauncherActor(container: Container, containerContext: ContainerLaunchContext, nodeManagerClient: NMClientAsync) extends Actor {
  val LOG = LogUtil.getLogger(getClass)

  override def preStart(): Unit = {
    nodeManagerClient.startContainerAsync(container, containerContext)
  }

  override def receive: Receive = {
    case _ =>
      LOG.error(s"Unknown message received")
  }  
}
