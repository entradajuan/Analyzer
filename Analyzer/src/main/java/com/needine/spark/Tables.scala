package com.needine.spark

import java.sql.Timestamp

object Tables {
  
  //case class Packet(time: Timestamp, origen:Double, destiny: Double, bytes: Double)extends Serializable
  
  case class Packet(time: Long, origen:Long, destiny: Long, bytes: Double)extends Serializable
  
  
  case class Origin_By_IP_TCP(ip: String, ref: Double)extends Serializable
  
  //case class Origin_By_Ref(ref: Int, ip: String)extends Serializable

  
}