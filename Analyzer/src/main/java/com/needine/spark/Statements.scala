package com.needine.spark

import com.datastax.driver.core.Session
import java.sql.Timestamp



object Statements {
  
  def saveTCPPacket(time: String, origen: Long, destiny: Long, bytes: Double): String = s"""
       insert into network_monitor.tcp_packet_by_origen_destiny (time, origen, destiny, bytes)
       values('$time',$origen,$destiny, $bytes)"""  
  
  def saveOriginByIP(ip: String, ref: Double): String = s"""
       insert into network_monitor.origin_by_ip_tcp (ip, ref)
       values('$ip', $ref)"""  
  
  def saveProtocol(name: String, ref: String): String = s"""
       insert into network_monitor.protocol (name, ref)
       values('$name','$ref')"""  
  
       
  def createKeySpace(session: Session) = {
    session.execute(
      """CREATE KEYSPACE  if not exists network_monitor WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };""")

    session.execute(
      """create table if not exists network_monitor.origin_by_ip_tcp (ip  text, ref double, primary key(ip))""")
 
    session.execute(
      """create table if not exists network_monitor.tcp_packet_by_origen_destiny (time text, origen bigint, destiny bigint, bytes double, primary key((origen, destiny), time))""")

    session.execute(
      """create table if not exists network_monitor.protocol ( name  text, ref text, primary key(name)) """)
      
  
  }
  
}