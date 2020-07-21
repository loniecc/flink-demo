package com.example.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * @author huzeming@sensorsdata.com
 * @version 1.0.0
 * @since 2020/07/21 17:16
 */

@Slf4j
@Component
public class Receiver {
  @KafkaListener(topics = {"Shakespeare"})
  public void listen(ConsumerRecord<?, ?> record) {

    Optional<?> kafkaMessage = Optional.ofNullable(record.value());

    if (kafkaMessage.isPresent()) {

      Object message = kafkaMessage.get();

      log.info("----------------- record =" + record);
      log.info("------------------ message =" + message);
    }

  }
}
