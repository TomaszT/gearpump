/*-
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

package org.apache.gearpump.experiments.yarn

import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.hadoop.yarn.util.Records
import org.slf4j.Logger
import org.apache.hadoop.yarn.util.Apps
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import java.io.File
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.io.DataOutputBuffer
import java.nio.ByteBuffer

import org.apache.gearpump.experiments.yarn.Constants._

case class ContainerLaunchContextFactory(yarnConf: YarnConfiguration, appConfig: AppConfig) {
  val LOG: Logger = LogUtil.getLogger(getClass)

  private def getFs(yarnConf: YarnConfiguration) = FileSystem.get(yarnConf)

  private def getAppEnv(yarnConf: YarnConfiguration): Map[String, String] = {
/*
    val appMasterEnv = new java.util.HashMap[String,String]
    for (
      c <- yarnConf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH.mkString(File.pathSeparator))
    ) {
      Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
        c.trim(), File.pathSeparator)
    }
    Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
      Environment.PWD.$()+File.separator+"*", File.pathSeparator)
    appMasterEnv.toMap
    */
    //Kam below is scala equivalent. It looks like it does the following
    // 1 get YARN_APPLICATION_CLASSPATH orElse DEFAULT_YARN_APPLICATION_CLASSPATH
    // 2 append current directory's contents to 1.
    // 3 return a Map with a single entry of CLASSPATH -> 1.join(';')
    val classPaths = yarnConf.getStrings(
      YarnConfiguration.YARN_APPLICATION_CLASSPATH,
      YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH.mkString(File.pathSeparator))
    val allPaths = classPaths :+ Environment.PWD.$()+File.separator+"*"+File.pathSeparator

    Map(Environment.CLASSPATH.name -> allPaths.reduceLeft((a,b) => {
      a + File.pathSeparator + b
    }))

  }

  private def getAMLocalResourcesMap: Map[String, LocalResource] = {
    val fs = getFs(yarnConf)
    val version = appConfig.getEnv("version")
    val hdfsRoot = appConfig.getEnv(HDFS_ROOT)
    Map(
      "pack" -> newYarnAppResource(fs, new Path(s"$hdfsRoot/$version.tar.gz"),
        LocalResourceType.ARCHIVE, LocalResourceVisibility.PUBLIC),
      "yarnConf" -> newYarnAppResource(fs, new Path(s"$hdfsRoot/conf"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC))
  }

  private def newYarnAppResource(fs: FileSystem, path: Path,
                                 resourceType: LocalResourceType, vis: LocalResourceVisibility): LocalResource = {
    val qualified = fs.makeQualified(path)
    val status = fs.getFileStatus(qualified)
    val resource = Records.newRecord(classOf[LocalResource])
    resource.setType(resourceType)
    resource.setVisibility(vis)
    resource.setResource(ConverterUtils.getYarnUrlFromPath(qualified))
    resource.setTimestamp(status.getModificationTime())
    resource.setSize(status.getLen())
    resource
  }


  private def getToken():ByteBuffer = {
    val credentials = UserGroupInformation.getCurrentUser.getCredentials
    val dob = new DataOutputBuffer
    credentials.writeTokenStorageToStream(dob)
    ByteBuffer.wrap(dob.getData)
  }

  private def logEnvironmentVars(environment: Map[String, String]) {
    environment.foreach(pair => {
      val (key, value) = pair
      LOG.info(s"getAppEnv key=$key value=$value")
    })
  }

  private def getContainerContext(command: String): ContainerLaunchContext = {
    val ctx = Records.newRecord(classOf[ContainerLaunchContext])
    ctx.setCommands(Seq(command))
    ctx.setEnvironment(getAppEnv(yarnConf))
    ctx.setTokens(getToken)
    ctx
  }

  def newInstance(command: String): ContainerLaunchContext = {
    val context = getContainerContext(command)
    context.setLocalResources(getAMLocalResourcesMap)
    context
  }

}