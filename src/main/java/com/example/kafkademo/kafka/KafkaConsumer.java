package com.example.kafkademo.kafka;

import com.example.kafkademo.entity.User;
import org.apache.kafka.clients.consumer.internals.AbstractCoordinator;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

@Service
public class KafkaConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(topics = "${spring.kafka.topic.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(User user){
        LOGGER.info(String.format("Json message recieved -> %s", user.toString()));
    }
}
