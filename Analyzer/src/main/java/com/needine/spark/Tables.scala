package com.needine.spark

import java.sql.Timestamp

object Tables {
  
  case class Packet(time: Timestamp, bytes: String)extends Serializable

  
}