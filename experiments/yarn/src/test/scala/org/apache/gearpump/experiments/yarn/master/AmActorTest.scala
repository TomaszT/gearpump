package org.apache.gearpump.experiments.yarn.master

import java.io.File

import akka.actor.{Props, ActorSystem}
import akka.testkit.{TestActorRef, TestKit}
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.main.ParseResult
import org.apache.gearpump.experiments.yarn.{ResourceManagerClientActor, AppConfig}
import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.scalatest.WordSpecLike
import org.specs2.matcher.MustMatchers
import scala.io.Source

class AmActorTest extends TestKit(ActorSystem("testsystem"))
with WordSpecLike
with MustMatchers
with StopSystemAfterAll {
  val YARN_TEST_CONFIG = "/gearpump_on_yarn.conf"
  val resource = getClass.getResource(YARN_TEST_CONFIG)
  println(resource.getPath)
  val config = ConfigFactory.parseFileAnySyntax(new File(resource.getPath))

  println("Config is empty : " + config.isEmpty)
  val appConfig = new AppConfig(new ParseResult(Map("version" -> "1.0"), Array.empty), config)
  val act = testActor
  //val amActor = system.actorOf(Props(classOf[AmActor], appConfig, new YarnConfiguration), "GearPumpAMActor")

  "An AmActor" when {
    "initialized (after preStart())" should {
      "initialized child actor" in {

        val yarnConfiguration = new YarnConfiguration
        //val rmClientActorProps = Props(new ResourceManagerClientActor(yarnConfiguration))
        val rmClientActorProps = Props(testActor.)
        val amActorProps = Props(new AmActor(appConfig, yarnConfiguration, AmActor.getRMCallbackHandlerActorProps, rmClientActorProps))
        val amActor = TestActorRef[AmActor](amActorProps)
        println(amActor.underlyingActor.version)

        //test state
        assert(true, "not implemented yet")

      }

/*
      "do something else" in {
        //other test (same state)
        assert(true, "not implemented yet")
      }
*/
    }
  }
}