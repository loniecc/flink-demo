package com.example.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Scanner;

/**
 * @author huzeming@sensorsdata.com
 * @version 1.0.0
 * @since 2020/07/21 17:20
 */
@SpringBootTest
class SenderTest {
  @Autowired
  private Sender sender;

  @Test
  public void testSend() throws InterruptedException {
    while (true){
      sender.send();
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}