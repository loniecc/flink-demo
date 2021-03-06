package com.example.kafka;

import lombok.Data;

import java.util.Date;

import javax.annotation.sql.DataSourceDefinition;

/**
 * @author huzeming@sensorsdata.com
 * @version 1.0.0
 * @since 2020/07/21 17:13
 */
@Data
public class Message {
  private Long id;    //id
  private String msg; //消息
  private Date sendTime;  //时间戳
}
