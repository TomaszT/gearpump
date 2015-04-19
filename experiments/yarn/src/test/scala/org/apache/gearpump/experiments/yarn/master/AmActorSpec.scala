package org.apache.gearpump.experiments.yarn.master

import java.io.File

import akka.actor.Actor.Receive
import akka.actor.{ActorRef, Actor, Props, ActorSystem}
import akka.japi.Creator
import akka.testkit.{TestProbe, TestActorRef, TestKit}
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.main.ParseResult
import org.apache.gearpump.experiments.yarn.{ResourceManagerClientActor, AppConfig}
import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.scalatest.WordSpecLike
import org.slf4j.Logger
import org.specs2.matcher.MustMatchers
import scala.io.Source

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

  "An AmActor" must {
    "resend the same message to RMClientActor when ContainerRequestMessage is send to it" in {
        val probe = TestProbe()
        val amActorProps = Props(new AmActor(appConfig, yarnConfiguration, AmActor.RMCallbackHandlerActorProps(Props(classOf[MockedChild], probe.ref)), AmActor.RMClientActorProps(Props(classOf[MockedChild], probe.ref))))
        val amActor = TestActorRef[AmActor](amActorProps)
        amActor ! ContainerRequestMessage(1024, 1)
        probe.expectMsg(ContainerRequestMessage(1024, 1))
      }
    }
}