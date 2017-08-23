package com.needine.spark

import com.datastax.driver.core.Session
import java.sql.Timestamp



object Statements {
  
  def savePacket(time: Timestamp, bytes: String): String = s"""
       insert into network_monitor.packet (time, bytes)
       values('$time', '$bytes')"""  
  
  def saveOriginByIP(ip: String, ref: Double): String = s"""
       insert into network_monitor.origin_by_ip_tcp (ip, ref)
       values('$ip', $ref)"""  
  
  
  def createKeySpace(session: Session) = {
    session.execute(
      """CREATE KEYSPACE  if not exists network_monitor WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };""")

    session.execute(
      """create table if not exists network_monitor.packet ( time timestamp, bytes  text, primary key((bytes), time) ) WITH CLUSTERING ORDER BY (time DESC)""")
  
    session.execute(
      """create table if not exists network_monitor.origin_by_ip_tcp (ip  text, ref double, primary key(ip))""")
 /*
    session.execute(
      """create table if not exists network_monitor.origin_by_ref (ref int, ip  text, primary key(ref))""")
*/
  
  }
  
}