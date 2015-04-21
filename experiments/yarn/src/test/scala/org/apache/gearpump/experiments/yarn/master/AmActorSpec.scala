package org.apache.gearpump.experiments.yarn.master

import java.io.File
import java.net.InetAddress

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.main.ParseResult
import org.apache.gearpump.experiments.yarn.AppConfig
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import org.slf4j.Logger
import org.specs2.matcher.MustMatchers
import org.apache.gearpump.experiments.yarn.Constants._
import scala.concurrent.duration._

class MockedChild(probe: ActorRef) extends Actor {
  val LOG: Logger = LogUtil.getLogger(getClass)

  override def receive: Receive = {
    case x =>  probe ! x
  }
}

class AmActorSpec extends TestKit(ActorSystem("testsystem"))
with WordSpecLike
with MustMatchers
with StopSystemAfterAll {
  import AmActorProtocol._

  val YARN_TEST_CONFIG = "/gearpump_on_yarn.conf"
  val resource = getClass.getResource(YARN_TEST_CONFIG)
  val config = ConfigFactory.parseFileAnySyntax(new File(resource.getPath))
  val appConfig = new AppConfig(new ParseResult(Map("version" -> "1.0"), Array.empty), config)
  val yarnConfiguration = new YarnConfiguration

  var probe:TestProbe = _
  var amActor: ActorRef = _

  override def beforeAll(): Unit = {
    println("before")
    probe = TestProbe()
    val amActorProps = Props(new AmActor(appConfig, yarnConfiguration, AmActor.RMCallbackHandlerActorProps(Props(classOf[MockedChild], probe.ref)), AmActor.RMClientActorProps(Props(classOf[MockedChild], probe.ref))))
    amActor = TestActorRef[AmActor](amActorProps)
  }


  "An AmActor" must {
    "resend the same message to RMClientActor when ContainerRequestMessage is send to it" in {
        amActor ! ContainerRequestMessage(1024, 1)
        probe.expectMsg(ContainerRequestMessage(1024, 1))
      }

    "forward the message and send RegisterAMMessage message to RMClientActor when ResourceManagerCallbackHandler is send to it" in {
      val msg = new ResourceManagerCallbackHandler(appConfig , probe.ref)
      probe.forward(amActor, msg)
      println(probe.lastSender)
      probe.expectMsg(msg)
      println(probe.lastSender)
      val port = appConfig.getEnv(YARNAPPMASTER_PORT).toInt
      val host = InetAddress.getLocalHost.getHostName
      val target = host + ":" + port
      val addr = NetUtils.createSocketAddr(target)
      val trackingURL = "http://" + host + ":" + appConfig.getEnv(SERVICES_PORT).toInt
      probe.expectMsg(new RegisterAMMessage(addr.getHostName, port, trackingURL))
      println(probe.lastSender)
    }
  }
}