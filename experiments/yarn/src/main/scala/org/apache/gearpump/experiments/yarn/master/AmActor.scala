/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.experiments.yarn.master

import java.util.concurrent.TimeUnit
import akka.actor._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import org.apache.gearpump.experiments.yarn.Actions._
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.slf4j.Logger
import scala.collection.JavaConverters._
import org.apache.gearpump.experiments.yarn.client.Client._
import org.apache.gearpump.experiments.yarn.CmdLineVars._
import org.apache.gearpump.experiments.yarn.EnvVars._
import java.net.InetAddress
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.ApplicationConstants
import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import org.apache.gearpump.experiments.yarn.AppConfig
import org.apache.gearpump.experiments.yarn.NodeManagerCallbackHandler
import org.apache.gearpump.experiments.yarn.YarnContainerUtil
import org.apache.gearpump.experiments.yarn.ResourceManagerClientActor
import org.apache.gearpump.experiments.yarn.master.ResourceManagerCallbackHandler


/**
 * Yarn ApplicationMaster.
 */
class AmActor(appConfig: AppConfig, yarnConf: YarnConfiguration) extends Actor {
  val LOG: Logger = LogUtil.getLogger(getClass)
  //TODO: should be generated (case of collision), there will be more than one master
  val MASTER_PORT = "3000"
  val CONTAINER_LOG_NAME = "container.log"  
  val nmCallbackHandler = createNMCallbackHandler
  val nmClientAsync = createNMClient(nmCallbackHandler)
  val rmCallbackHandler = context.actorOf(Props(classOf[RMCallbackHandlerActor], appConfig, self), "rmCallbackHandler")
  val amRMClient = context.actorOf(Props(classOf[ResourceManagerClientActor], yarnConf, self), "amRMClient")
  val containersStatus = collection.mutable.Map[Long, ContainerInfo]()
  
  override def receive: Receive = {
    case containerRequest: ContainerRequestMessage =>
      LOG.info("AM: Received ContainerRequestMessage")
      amRMClient ! containerRequest
    
    case rmCallbackHandler: ResourceManagerCallbackHandler =>
      LOG.info("Received RMCallbackHandler")
      amRMClient forward rmCallbackHandler
      val host = InetAddress.getLocalHost().getHostName();
      val port = appConfig.getEnv(YARNAPPMASTER_PORT).toInt
      val target = host + ":" + port
      val addr = NetUtils.createSocketAddr(target);
      amRMClient ! RegisterAMMessage(addr.getHostName, port, "")
    
    case amResponse: RegisterApplicationMasterResponse =>
      LOG.info("Received RegisterApplicationMasterResponse")
      requestContainers(amResponse)

    case launchMasterContainers: LaunchMasterContainers =>
      LOG.info("Received LaunchMasterContainers")
      launchContainers(launchMasterContainers.containers, getMasterCommand)
    
    case done: RMHandlerDone =>
      cleanUp(done)
  }

  private[this] def launchContainers(containers: List[Container], getCommand: String => String) {
    containers.foreach(container => {
      container.getNodeId.getHost
      LOG.info(s"Launching containter: containerId :  ${container.getId}, host ip : ${container.getNodeId.getHost}")
      val command = getCommand(getCliOptsForMasterAddr(container.getNodeId.getHost, MASTER_PORT))
      LOG.info("Launching command : " + command)
      context.actorOf(Props(classOf[ContainerLauncherActor], container, nmClientAsync, yarnConf, command))
    })
  }
  /**
   * TODO: ip and port should be constants   
   */
  private[this] def getCliOptsForMasterAddr(masterHost:String, masterPort:String): String = {
    //ie: -ip 127.0.0.1 -port 3000
    return s"-ip $masterHost -port $masterPort"
  }

  private[this] def getMasterCommand(cliOpts: String): String = {
    val exe = appConfig.getEnv(GEARPUMPMASTER_COMMAND)
    val main = appConfig.getEnv(GEARPUMPMASTER_MAIN)
    val command = s"$exe $main $cliOpts 2>&1> ${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/$CONTAINER_LOG_NAME"     
    command
  }

  private[this] def getWorkerCommand(cliOpts: String): String = {
    val exe = appConfig.getEnv(WORKER_COMMAND)
    val main = appConfig.getEnv(WORKER_MAIN)
    val command = s"$exe $main $cliOpts 2>&1> ${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/$CONTAINER_LOG_NAME"     
    command
  }

  private[this] def createNMClient(containerListener: NodeManagerCallbackHandler): NMClientAsync = {
    LOG.info("Creating NMClientAsync")
    val nmClient = new NMClientAsyncImpl(containerListener)
    LOG.info("Yarn config : " + yarnConf.get("yarn.resourcemanager.hostname"))
    nmClient.init(yarnConf)
    nmClient.start()
    nmClient
  }

  private[this] def createNMCallbackHandler: NodeManagerCallbackHandler = {
    LOG.info("Creating NMCallbackHandler")
    NodeManagerCallbackHandler()
  }

  private[this] def requestContainers(registrationResponse: RegisterApplicationMasterResponse) {
    val previousContainersCount = registrationResponse.getContainersFromPreviousAttempts.size
    
    LOG.info(s"Previous container count : $previousContainersCount")
    if(previousContainersCount > 0) {
      LOG.warn("Previous container count > 0, can't do anything with it")
    }
    
    (1 to appConfig.getEnv(CONTAINER_COUNT).toInt).foreach(requestId => {
      amRMClient ! ContainerRequestMessage(appConfig.getEnv(CONTAINER_MEMORY).toInt, appConfig.getEnv(CONTAINER_VCORES).toInt)
    })

  }

  private[this] def cleanUp(done: RMHandlerDone): Boolean = {
    LOG.info("Application completed. Stopping running containers")
    nmClientAsync.stop()
    var success = true

    val stats = done.rMHandlerContainerStats
    done.reason match {
      case failed: Failed =>
        val message = s"Failed. total=${appConfig.getEnv(CONTAINER_COUNT).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
        amRMClient ! AMStatusMessage(FinalApplicationStatus.FAILED, message, null)
        success = false
      case ShutdownRequest =>
        if (stats.failed == 0 && stats.completed == appConfig.getEnv(CONTAINER_COUNT).toInt) {
          val message = s"ShutdownRequest. total=${appConfig.getEnv(CONTAINER_COUNT).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
          amRMClient ! AMStatusMessage(FinalApplicationStatus.KILLED, message, null)
          success = false
        } else {
          val message = s"ShutdownRequest. total=${appConfig.getEnv(CONTAINER_COUNT).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
          amRMClient ! AMStatusMessage(FinalApplicationStatus.FAILED, message, null)
          success = false
        }
       case AllRequestedContainersCompleted =>
        val message = s"Diagnostics. total=${appConfig.getEnv(CONTAINER_COUNT).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
        amRMClient ! AMStatusMessage(FinalApplicationStatus.SUCCEEDED, message, null)
        success = true
    }

    amRMClient ! PoisonPill
    success
    }
}
 
class RMCallbackHandlerActor(appConfig: AppConfig, yarnAM: ActorRef) extends Actor {
  val LOG: Logger = LogUtil.getLogger(getClass)
  val rmCallbackHandler = new ResourceManagerCallbackHandler(appConfig, yarnAM)

  override def preStart(): Unit = {
    LOG.info("Sending RMCallbackHandler to YarnAM")
    yarnAM ! rmCallbackHandler
  }

  override def receive: Receive = {
    case _ =>
      LOG.error(s"Unknown message received")
  }

}


object YarnAM extends App with ArgumentsParser {
  val LOG: Logger = LogUtil.getLogger(getClass)
  val TIME_INTERVAL = 1000

  override val options: Array[(String, CLIOption[Any])] = Array(
    APPMASTER_IP -> CLIOption[String]("<Gearpump master ip>", required = false),
    APPMASTER_PORT -> CLIOption[String]("<Gearpump master port>", required = false)
  )

  /**
   * For yet unknown reason this is needed for my local pseudo distributed cluster.   
   * 
   */
  def getForcedDefaultYarnConf:Configuration = {
      val hadoopConf  = new Configuration(true)
      val configDir = System.getenv("HADOOP_CONF_DIR")
      Configuration.addDefaultResource(configDir + "/core-site.xml")
      Configuration.addDefaultResource(configDir + "/hdfs-site")
      Configuration.addDefaultResource(configDir + "/yarn-site.xml")
      new YarnConfiguration(hadoopConf)
  }
  
  def apply(args: Array[String]) = {
    try {
      implicit val timeout = Timeout(5, TimeUnit.SECONDS)
      val config = ConfigFactory.load
      implicit val system = ActorSystem("GearPumpAM", config)
      val appConfig = new AppConfig(parse(args), config)
      val yarnConfiguration = getForcedDefaultYarnConf
      LOG.info("HADOOP_CONF_DIR: " + System.getenv("HADOOP_CONF_DIR"))
      LOG.info("Yarn config (yarn.resourcemanager.hostname): " + yarnConfiguration.get("yarn.resourcemanager.hostname"))
      LOG.info("Creating AMActor v1.0")
      system.actorOf(Props(classOf[AmActor], appConfig, yarnConfiguration), "GearPumpAMActor")
      system.awaitTermination()
      LOG.info("Shutting down")
      system.shutdown()
    } catch {
      case throwable: Throwable =>
        LOG.error("Caught exception", throwable)
        throwable.printStackTrace()
    }

  }

  apply(args)

}