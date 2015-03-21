package org.apache.gearpump.experiments.yarn

import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus

object Actions {
  sealed trait Reason
  case class Failed(throwable: Throwable) extends Reason
  case object ShutdownRequest extends Reason
  case object AllRequestedContainersCompleted extends Reason
  
  sealed trait ContainerType
  case object Master extends ContainerType
  case object Woker extends ContainerType
  case object Service extends ContainerType

  sealed trait ContainerState
  case object Requested extends ContainerState
  case object Completed extends ContainerState
  case object Allocated extends ContainerState
  //failed?
  
  case class LaunchMasterContainers(containers: List[Container])
  case class LaunchWorkerContainers(containers: List[Container])
  case class LaunchServiceContainer(containers: List[Container])
  case class ContainerRequestMessage(memory: Int, vCores: Int)
  case class RMHandlerDone(reason: Reason, rMHandlerContainerStats: RMHandlerContainerStats)
  case class RMHandlerContainerStats(allocated: Int, completed: Int, failed: Int)
  case class RegisterAMMessage(appHostName: String, appHostPort: Int, appTrackingUrl: String)
  case class AMStatusMessage(appStatus: FinalApplicationStatus, appMessage: String, appTrackingUrl: String)
  case class ContainerInfo(container:Container, containerType: ContainerType, launchCommand: String => String)
}
