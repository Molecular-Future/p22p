package org.mos.p22p.core

import com.google.protobuf.Message
import onight.oapi.scala.traits.OLog
import onight.osgi.annotation.NActorProvider
import onight.tfw.ntrans.api.ActorService
import org.apache.felix.ipojo.annotations.{Instantiate, Provides}

@NActorProvider
@Instantiate(name = "pzpctrl")
@Provides(specifications = Array(classOf[ActorService]), strategy = "SINGLETON")
class PZPCtrl extends PSMPZP[Message] with OLog {

  def networkByID(netid: String): Network = {
    Networks.networkByID(netid);
  }
  
  override def getCmds: Array[String] = Array("CTL");
  
}

