package org.apache.gearpump.experiments.yarn.master

import java.io.File
import java.net.InetAddress
import java.nio.ByteBuffer

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.main.ParseResult
import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.gearpump.experiments.yarn.master.AmActorProtocol.{ContainerRequestMessage, RegisterAppMasterResponse, AMRMClientAsyncStartup, RegisterAMMessage}
import org.apache.gearpump.experiments.yarn.{AppConfig, NodeManagerCallbackHandlerFactory}
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.mockito.Mockito
import org.scalatest._
import Matchers._
import org.slf4j.Logger
import org.specs2.matcher.MustMatchers

import scala.util.{Success, Try}

class MockedChild(probe: ActorRef) extends Actor {
  val LOG: Logger = LogUtil.getLogger(getClass)

  override def receive: Receive = {
    case x =>  probe ! x
  }
}

class ResourceManagerClientActorMock(sendBack: ActorRef) extends Actor {
  val LOG: Logger = LogUtil.getLogger(getClass)

  override def receive: Receive = {
    case rmCallbackHandler: ResourceManagerCallbackHandler =>
      LOG.info("Received ResourceManagerCallbackHandler")
      sendBack ! AMRMClientAsyncStartup(Try(true))
    case amAttr: RegisterAMMessage =>
      LOG.info(s"Received RegisterAMMessage! ${amAttr.appHostName}:${amAttr.appHostPort}${amAttr.appTrackingUrl}")
      val response = Mockito.mock(classOf[RegisterApplicationMasterResponse])
      sendBack ! RegisterAppMasterResponse(response=response)
    case containerRequest: ContainerRequestMessage =>
      LOG.info(s"Received containerRequest")
    case unknown =>
      LOG.info(s"Unknown message type ${unknown.getClass.getName}")
      sender ! unknown
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

  val probe = TestProbe()
  val amActor = createAmActor

  private[this] def getRegisterAMMessage: RegisterAMMessage = {
    val port = appConfig.getEnv(YARNAPPMASTER_PORT).toInt
    val host = InetAddress.getLocalHost.getHostName
    val target = host + ":" + port
    val addr = NetUtils.createSocketAddr(target)
    val trackingURL = "http://" + host + ":" + appConfig.getEnv(SERVICES_PORT).toInt
    new RegisterAMMessage(addr.getHostName, port, trackingURL)
  }

  private[this] def createAmActor: TestActorRef[AmActor] = {
    val amActorProps = Props(new AmActor(appConfig,
      yarnConfiguration,
      AmActor.RMCallbackHandlerActorProps(Props(classOf[MockedChild], probe.ref)),
      AmActor.RMClientActorProps(Props(classOf[ResourceManagerClientActorMock], testActor)),
    //those will be changed to mockito mocks
      NMClientAsyncFactory(),
      NodeManagerCallbackHandlerFactory()
    ))
    TestActorRef[AmActor](amActorProps)
  }

  "An AmActor" should "send ResourceManagerCallbackHandler to ResourceManagerClientActor and receive a AMRMClientAsyncStartup(status=Try(true))" in {
    val msg = new ResourceManagerCallbackHandler(appConfig, probe.ref)
    val response = AMRMClientAsyncStartup(status = Try(true))
    amActor ! msg
    expectMsg(response)
  }

  "An AmActor" should "send RegisterAMMessage to ResourceManagerClientActor and receive a RegisterAppMasterResponse" in {
    val msg = AMRMClientAsyncStartup(status=Success(true))
    val response = RegisterAppMasterResponse(response=Mockito.mock(classOf[RegisterApplicationMasterResponse])).getClass
    amActor ! msg
    expectMsgType[RegisterAppMasterResponse]
  }

  "An AmActor" should "when receiving a RegisterAppMasterResponse it should send one or more ContainerRequestMessages to ResourceManagerClientActor" in {
    val msg = ContainerRequestMessage(appConfig.getEnv(GEARPUMPMASTER_MEMORY).toInt, appConfig.getEnv(GEARPUMPMASTER_VCORES).toInt)
    amActor ! msg
    expectNoMsg
  }

  ignore should "when LaunchContainers is send to it" in {

  }
  ignore should "when RMHandlerDone is send to it" in {

  }
  ignore should "when ContainerStarted is send to it" in {

  }

  "An AmActor" should  "resend the same message to RMClientActor when ContainerRequestMessage is send to it" in {
    amActor ! ContainerRequestMessage(1024, 1)
    //TODO setup correct probe
    expectNoMsg
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
//    probe.expectMsg(ContainerRequestMessage(appConfig.getEnv(WORKER_MEMORY).toInt, appConfig.getEnv(WORKER_VCORES).toInt))
  }

  private def getContainerId():ContainerId = {
    ContainerId.newInstance(ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1), 1)
  }
}