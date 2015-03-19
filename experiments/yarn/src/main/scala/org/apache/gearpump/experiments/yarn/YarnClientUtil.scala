package org.apache.gearpump.experiments.yarn

import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.yarn.api.records.LocalResourceType
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.hadoop.yarn.util.Records
import org.slf4j.Logger
import org.apache.hadoop.yarn.util.Apps
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import java.io.File
import scala.collection.JavaConversions._
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.io.DataOutputBuffer
import java.nio.ByteBuffer


object YarnClientUtil {
  val LOG: Logger = LogUtil.getLogger(getClass)
  val HDFS_JARS_DIR = "/user/gearpump/jars/"
  
  def getFs(yarnConf: YarnConfiguration) = FileSystem.get(yarnConf)  
  def getHdfs(yarnConf: YarnConfiguration) = new Path(FileSystem.get(yarnConf).getHomeDirectory, HDFS_JARS_DIR)

  def getAppEnv(yarnConf: YarnConfiguration): Map[String, String] = {
    val appMasterEnv = new java.util.HashMap[String,String]
    for (
      c <- yarnConf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH.mkString(","))
    ) {
      Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
        c.trim(), File.pathSeparator)
    }
    Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
      Environment.PWD.$()+File.separator+"*", File.pathSeparator)
    appMasterEnv.toMap
  }

  def getAMLocalResourcesMap(yarnConf: YarnConfiguration): Map[String, LocalResource] = {
      val hdfs = getHdfs(yarnConf)
      getFs(yarnConf).listStatus(hdfs).map(fileStatus => {
      val localResouceFile = Records.newRecord(classOf[LocalResource])
      val path = ConverterUtils.getYarnUrlFromPath(fileStatus.getPath)
      LOG.info(s"local resource path=${path.getFile}")
      localResouceFile.setResource(path)
      localResouceFile.setType(LocalResourceType.FILE)
      localResouceFile.setSize(fileStatus.getLen)
      localResouceFile.setTimestamp(fileStatus.getModificationTime)
      localResouceFile.setVisibility(LocalResourceVisibility.APPLICATION)
      fileStatus.getPath.getName -> localResouceFile
    }).toMap
  }

  def getContext(yarnConf: YarnConfiguration, container: Container, command:String): ContainerLaunchContext = {    
    val ctx = Records.newRecord(classOf[ContainerLaunchContext])
    ctx.setCommands(Seq(command))
    
    val environment = getAppEnv(yarnConf) 
    environment.foreach(pair => {
      val (key, value) = pair
      LOG.info(s"getAppEnv key=$key value=$value")
    })
    ctx.setEnvironment(environment)
    ctx.setLocalResources(getAMLocalResourcesMap(yarnConf))
    val credentials = UserGroupInformation.getCurrentUser.getCredentials
    val dob = new DataOutputBuffer
    credentials.writeTokenStorageToStream(dob)
    ctx.setTokens(ByteBuffer.wrap(dob.getData))
    ctx
  }

}