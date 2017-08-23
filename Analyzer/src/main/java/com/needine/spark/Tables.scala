package com.needine.spark

import java.sql.Timestamp

object Tables {
  
  case class Packet(time: Timestamp, bytes: String)extends Serializable
  
  case class Origin_By_IP_TCP(ip: String, ref: Double)extends Serializable
  
  //case class Origin_By_Ref(ref: Int, ip: String)extends Serializable

  
}