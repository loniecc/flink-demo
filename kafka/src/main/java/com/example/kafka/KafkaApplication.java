package com.example.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaApplication {

  @Autowired
  public static void main(String[] args) {
    SpringApplication.run(KafkaApplication.class, args);
  }
}
