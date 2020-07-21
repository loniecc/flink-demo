package com.example.kafka;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Random;
import java.util.UUID;

/**
 * @author huzeming@sensorsdata.com
 * @version 1.0.0
 * @since 2020/07/21 17:14
 */
@Component
@Slf4j
public class Sender {

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  private Gson gson = new GsonBuilder().create();

  Random random = new Random();
  //发送消息方法
  public void send() {
    Message message = new Message();
    message.setId(System.currentTimeMillis());
    message.setMsg(random.nextInt(100)+"");
    message.setSendTime(new Date());
    log.info("+++++++++++++++++++++  message = {}", gson.toJson(message));
    kafkaTemplate.send("Shakespeare", gson.toJson(message));
  }
}
