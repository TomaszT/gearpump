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

package org.apache.gearpump.cluster.main

import akka.actor.{ActorSystem, Props}
import org.apache.gearpump.cluster.ClusterConfig
import org.apache.gearpump.cluster.master.MasterProxy
import org.apache.gearpump.cluster.worker.{Worker=>WorkerActor}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.LogUtil
import org.apache.gearpump.util.LogUtil.ProcessType
import org.slf4j.Logger

import scala.collection.JavaConverters._

object
Worker extends App with ArgumentsParser {
  val config = ClusterConfig.load.worker
  val LOG : Logger = {
    LogUtil.loadConfiguration(config, ProcessType.WORKER)
    //delay creation of LOG instance to avoid creating an empty log file as we reset the log file name here
    LogUtil.getLogger(getClass)
  }

  def uuid = java.util.UUID.randomUUID.toString
  val options: Array[(String, CLIOption[Any])] = 
    Array("ip"->CLIOption[String]("<master ip address>",required = false),
      "port"->CLIOption("<master port>",required = false))
  
  def start(): Unit = {
    worker()
  }

  def worker(): Unit = {
    val id = uuid
    val system = ActorSystem(id, config)
    val mastersAddresses = getMastersAddresses
    LOG.info(s"Trying to connect to masters " + mastersAddresses.mkString(",") + "...")
    val masterProxy = system.actorOf(MasterProxy.props(mastersAddresses), MASTER)

    system.actorOf(Props(classOf[WorkerActor], masterProxy),
      classOf[WorkerActor].getSimpleName + id)

    system.awaitTermination()
  }

  def getMasterAddressFromArgs:Iterable[HostPort] = {
       val cmdLineConfig = parse(args)
       if(cmdLineConfig.exists("ip") && cmdLineConfig.exists("port")) {
         Seq(HostPort(cmdLineConfig.getString("ip"), cmdLineConfig.getInt("port")))
       } else {
         Seq.empty
       }
  }
  
  def getMastersAddressesFromConfig:Iterable[HostPort] = {
    config.getStringList("gearpump.cluster.masters").asScala.map { address =>
      val hostAndPort = address.split(":")
      HostPort(hostAndPort(0), hostAndPort(1).toInt)
    }
  }

  def getMastersAddresses:Iterable[HostPort] = {
    val masterAddressFromArgs = getMasterAddressFromArgs
    if(!masterAddressFromArgs.isEmpty) masterAddressFromArgs else getMastersAddressesFromConfig
  }
  start()
}
