package org.apache.gearpump.experiments.yarn.master

import java.io.File
import java.net.InetAddress

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.main.ParseResult
import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.gearpump.experiments.yarn.{AppConfig, NodeManagerCallbackHandlerFactory}
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, ApplicationId, ContainerId}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.scalatest._
import Matchers._
import org.slf4j.Logger
import org.specs2.matcher.MustMatchers

class MockedChild(probe: ActorRef) extends Actor {
  val LOG: Logger = LogUtil.getLogger(getClass)

  override def receive: Receive = {
    case x =>  probe ! x
  }
}

class AmActorSpec extends TestKit(ActorSystem("testsystem"))
with FlatSpecLike
with MustMatchers
with StopSystemAfterAll
with BeforeAndAfter {
  import AmActorProtocol._

  val YARN_TEST_CONFIG = "/gearpump_on_yarn.conf"
  val resource = getClass.getResource(YARN_TEST_CONFIG)
  val config = ConfigFactory.parseFileAnySyntax(new File(resource.getPath))
  val MASTER_CONTAINERS = 3
  val appConfig = new AppConfig(new ParseResult(Map("version" -> "1.0", GEARPUMPMASTER_CONTAINERS -> MASTER_CONTAINERS.toString), Array.empty), config)

  val yarnConfiguration = new YarnConfiguration

  var probe:TestProbe = _
  var amActor: TestActorRef[AmActor] = _

  before {
    probe = TestProbe()
    val amActorProps = Props(new AmActor(appConfig,
      yarnConfiguration,
      AmActor.RMCallbackHandlerActorProps(Props(classOf[MockedChild], probe.ref)),
      AmActor.RMClientActorProps(Props(classOf[MockedChild], probe.ref)),
    //those will be changed to mockito mocks
      NMClientAsyncFactory(),
      NodeManagerCallbackHandlerFactory()
    ))
    amActor = TestActorRef[AmActor](amActorProps)
  }


  "An AmActor" should
    "resend the same message to RMClientActor when ContainerRequestMessage is send to it" in {
    amActor ! ContainerRequestMessage(1024, 1)
    probe.expectMsg(ContainerRequestMessage(1024, 1))
  }

  it should "forward the message and send RegisterAMMessage message to RMClientActor when ResourceManagerCallbackHandler is send to it" in {
    val msg = new ResourceManagerCallbackHandler(appConfig , probe.ref)
    amActor ! msg
    probe.expectMsg(msg)
    val port = appConfig.getEnv(YARNAPPMASTER_PORT).toInt
    val host = InetAddress.getLocalHost.getHostName
    val target = host + ":" + port
    val addr = NetUtils.createSocketAddr(target)
    val trackingURL = "http://" + host + ":" + appConfig.getEnv(SERVICES_PORT).toInt
    probe.expectMsg(new RegisterAMMessage(addr.getHostName, port, trackingURL))
    println(probe.lastSender)
  }



  ignore should "when RegisterApplicationMasterResponse is send to it" in {

  }

  ignore should "when LaunchContainers is send to it" in {

  }
  ignore should "when RMHandlerDone is send to it" in {

  }
  ignore should "when ContainerStarted is send to it" in {

  }

  "An AmActor without enough masters started" should "increment started masters counter when ContainerStarted is send to it" in {
    amActor.underlyingActor.masterContainersStarted = 0
    amActor ! ContainerStarted(getContainerId())
    amActor.underlyingActor.masterContainersStarted shouldBe 1
  }

  "An AmActor that just started enough masters" should "increment started masters counter and request worker containers when ContainerStarted is send to it" in {
    amActor.underlyingActor.masterContainersStarted = MASTER_CONTAINERS - 1
    amActor.underlyingActor.workerContainersStarted = 0
    amActor ! ContainerStarted(getContainerId())
    amActor.underlyingActor.masterContainersStarted shouldBe MASTER_CONTAINERS
    probe.expectMsg(ContainerRequestMessage(appConfig.getEnv(WORKER_MEMORY).toInt, appConfig.getEnv(WORKER_VCORES).toInt))
  }

  private def getContainerId():ContainerId = {
    ContainerId.newInstance(ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1), 1)
  }
}