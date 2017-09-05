package com.needine.spark

import java.sql.Timestamp

object Tables {
  
  case class TCPPacket(time: String, origen:Long, destiny: Long, bytes: Double)extends Serializable
  
  case class Protocol(name: String, ref: String)extends Serializable
  
  case class Origin_By_IP_TCP(ip: String, ref: Double)extends Serializable
 

  
}