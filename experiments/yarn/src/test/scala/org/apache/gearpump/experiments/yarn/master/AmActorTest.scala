package org.apache.gearpump.experiments.yarn.master

import akka.actor.{Props, ActorSystem}
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.experiments.yarn.AppConfig
import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.scalatest.WordSpecLike
import org.specs2.matcher.MustMatchers

class AmActorTest extends TestKit(ActorSystem("testsystem"))
with WordSpecLike
with MustMatchers
with StopSystemAfterAll {

  val config = ConfigFactory.parseResourcesAnySyntax(YARN_CONFIG)
  val appConfig = new AppConfig(null, config)
  val amActor = system.actorOf(Props(classOf[AmActor], appConfig, new YarnConfiguration), "GearPumpAMActor")

  "An AmActor" when {
    "initialized (after preStart())" should {
      "initialized child actor" in {
        //val amActor = TestActorRef[AmActor]

        //test state
        //assert(true, "not implemented yet")
        fail("")
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