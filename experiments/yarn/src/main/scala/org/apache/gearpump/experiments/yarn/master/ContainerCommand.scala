package org.apache.gearpump.experiments.yarn.master

import java.io.File

import org.apache.gearpump.experiments.yarn.AppConfig
import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.gearpump.util.Constants
import org.apache.hadoop.yarn.api.ApplicationConstants


abstract class ContainerCommand {
  val appConfig: AppConfig
  val version = appConfig.getEnv("version")
  val classPath = Array(s"pack/$version/conf", s"pack/$version/dashboard", s"pack/$version/lib/*")

  def getCommand:String

  protected def buildCommand(java: String, properties: Array[String], mainProp: String, cliOpts: String, lognameProp: String):String = {
    val exe = appConfig.getEnv(java)
    val main = appConfig.getEnv(mainProp)
    val logname = appConfig.getEnv(lognameProp)
    s"$exe -cp ${classPath.mkString(File.pathSeparator)}${File.pathSeparator}" +
      "$CLASSPATH " + properties.mkString(" ") +
      s"  $main $cliOpts 2>&1 | /usr/bin/tee -a ${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/$logname"
  }
}


case class MasterContainerCommand(appConfig: AppConfig, masterHost: String, masterPort: Int) extends ContainerCommand {

  def getCommand: String = {
    val masterArguments = s"-ip $masterHost -port $masterPort"

    val properties = Array(
      s"-D${Constants.GEARPUMP_CLUSTER_MASTERS}.0=${masterHost}:${masterPort}",
      s"-D${Constants.GEARPUMP_LOG_DAEMON_DIR}=${ApplicationConstants.LOG_DIR_EXPANSION_VAR}",
      s"-D${Constants.GEARPUMP_LOG_APPLICATION_DIR}=${ApplicationConstants.LOG_DIR_EXPANSION_VAR}")

    buildCommand(GEARPUMPMASTER_COMMAND, properties, GEARPUMPMASTER_MAIN,
      masterArguments, GEARPUMPMASTER_LOG)
  }
}

case class WorkerContainerCommand(masterContainerCommand: MasterContainerCommand, workerHost: String) extends ContainerCommand {

  override val appConfig: AppConfig = masterContainerCommand.appConfig

  def getCommand: String = {
    val properties = Array(
      s"-D${Constants.GEARPUMP_CLUSTER_MASTERS}.0=${masterContainerCommand.masterHost}:${masterContainerCommand.masterPort}",
      s"-D${Constants.GEARPUMP_LOG_DAEMON_DIR}=${ApplicationConstants.LOG_DIR_EXPANSION_VAR}",
      s"-D${Constants.GEARPUMP_LOG_APPLICATION_DIR}=${ApplicationConstants.LOG_DIR_EXPANSION_VAR}",
      s"-D${Constants.GEARPUMP_HOSTNAME}=$workerHost")

    buildCommand(WORKER_COMMAND, properties,  WORKER_MAIN, "", WORKER_LOG)
  }


}
