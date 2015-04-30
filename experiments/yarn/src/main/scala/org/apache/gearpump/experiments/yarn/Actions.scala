package org.apache.gearpump.experiments.yarn

import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.api.records.ContainerId

//Kam maybe rename to AmStates or something - similar to AmActorProtocol
// You could also move this into AmActor.scala
object Actions {
  sealed trait Reason
  case class Failed(throwable: Throwable) extends Reason
  case object ShutdownRequest extends Reason
  case object AllRequestedContainersCompleted extends Reason
  
  sealed trait ContainerType
  case object Master extends ContainerType
  //Kam these don't seem to be used
  case object Woker extends ContainerType
  case object Service extends ContainerType

  //Kam same with these I think they're now under AmActorProtocol
  sealed trait YarnApplicationMasterState
  case object RequestingMasters extends YarnApplicationMasterState
  case object RequestingWorkers extends YarnApplicationMasterState
  case object RequestingService extends YarnApplicationMasterState
  



}
