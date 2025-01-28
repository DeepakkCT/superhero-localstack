package com.example.superhero_localstack.service;


import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.example.superhero_localstack.model.Superhero;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class SuperheroMessageProducer {

    private final AmazonSQS amazonSQS;
    private final ObjectMapper objectMapper;
    private static final String QUEUE_URL = "http://localhost:4566/000000000000/superhero-queue";

    @Autowired
    public SuperheroMessageProducer(AmazonSQS amazonSQS, ObjectMapper objectMapper) {
        this.amazonSQS = amazonSQS;
        this.objectMapper = objectMapper;
    }

    public void sendMessage(Superhero superhero) {
        try {
            String message = objectMapper.writeValueAsString(superhero);
            SendMessageRequest sendMessageRequest = new SendMessageRequest()
                    .withQueueUrl(QUEUE_URL)
                    .withMessageBody(message);
            amazonSQS.sendMessage(sendMessageRequest);
        } catch (Exception e) {
            throw new RuntimeException("Error sending message to SQS", e);
        }
    }


    public void sendUpdateMessage(Superhero superhero) {
        try {
            String message = objectMapper.writeValueAsString(superhero);
            Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
            messageAttributes.put("operation", new MessageAttributeValue()
                    .withDataType("String")
                    .withStringValue("UPDATE"));

            SendMessageRequest sendMessageRequest = new SendMessageRequest()
                    .withQueueUrl(QUEUE_URL)
                    .withMessageBody(message)
                    .withMessageAttributes(messageAttributes);

            amazonSQS.sendMessage(sendMessageRequest);
        } catch (Exception e) {
            throw new RuntimeException("Error sending update message to SQS", e);
        }
    }
}
