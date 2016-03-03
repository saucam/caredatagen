package com.guavus.care.datagen

/**
 * Created by yash.datta on 02/03/16.
 */
class Data {

}

case class TimeS(val exporttimestamp: Int, val timestamp: Int)
/*
 *
 * CREATE  external TABLE `f___edr_bin_source___3600_edr_flow_subs`(

  `msisdn` bigint,

   `device` string,

   `OS_Id` string,

    `App_Id` string,

  `flow_down_bytes` bigint,

  `flow_up_bytes` bigint,

  `roaming_session_count` bigint,

  `session_count` bigint,

  `roaming_session_duration` bigint,

  `total_session_duration` bigint,

  `roaming_flow_down_bytes` bigint,

  `roaming_flow_up_bytes` bigint,

   `QOE` bigint)

PARTITIONED BY (

  `exporttimestamp` bigint,

  `timestamp` bigint) stored as parquet
 *
 */

case class Cube1(val msisdn: Long, val deviceId: Long,
  val OS_Id: String, val App_Id: Long, val flow_down_bytes: Long,
  val flow_up_bytes: Long, val roaming_session_count: Long,
  val session_count: Long, val roaming_session_duration: Long,
  val total_session_duration: Long, val roaming_flow_down_bytes: Long,
  val roaming_flow_up_bytes: Long, val QOE: Long, val exporttimestamp: Int, val timestamp: Int)


case class Cube2(val msisdn: Long, val deviceId: Long,
  val OS_Id: String, val App_Id: Long, val flow_down_bytes: Long,
  val flow_up_bytes: Long, val roaming_session_count: Long,
  val session_count: Long, val roaming_session_duration: Long,
  val total_session_duration: Long, val roaming_flow_down_bytes: Long,
  val roaming_flow_up_bytes: Long, val QOE: Long,
  val peer_flow_down_bytes: Long, val peer_flow_up_bytes: Long,
  val peer_roaming_session_count: Long, val peer_session_count: Long,
  val peer_roaming_session_duration: Long, val peer_total_session_duration: Long,
  val peer_roaming_flow_down_bytes: Long, val peer_roaming_flow_up_bytes: Long,
  val peer_QOE: Long)
