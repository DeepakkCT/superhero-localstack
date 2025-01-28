package com.example.superhero_localstack.service;


import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.example.superhero_localstack.model.Superhero;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Data
@Service
@Slf4j
public class SuperheroMessageConsumer {
    private static final String QUEUE_URL = "http://localhost:4566/000000000000/superhero-queue";

    private final AmazonSQS amazonSQS;
    private final ObjectMapper objectMapper;
    private final SuperheroService superheroService;

    @Autowired
    public SuperheroMessageConsumer(AmazonSQS amazonSQS, ObjectMapper objectMapper, SuperheroService superheroService) {
        this.amazonSQS = amazonSQS;
        this.objectMapper = objectMapper;
        this.superheroService = superheroService;
        System.out.println("Consumer initialized");

    }

    @Scheduled(fixedRate = 5000)
    public void consumeMessages() {

        try {
            ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest()
                    .withQueueUrl(QUEUE_URL)
                    .withMaxNumberOfMessages(10)
                    .withMessageAttributeNames("All")
                    .withWaitTimeSeconds(10);

            List<Message> messages = amazonSQS.receiveMessage(receiveRequest).getMessages();



            for (Message message : messages) {
                try {
                    processMessage(message);
                    amazonSQS.deleteMessage(QUEUE_URL, message.getReceiptHandle());
                } catch (Exception e) {
                    System.err.println("Error processing message: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            System.err.println("Error in consumer: " + e.getMessage());

            e.printStackTrace();
        }
    }

    private void processMessage(Message message) throws Exception {
        String messageBody = message.getBody();


        Superhero superhero = objectMapper.readValue(messageBody, Superhero.class);
        Map<String, MessageAttributeValue> messageAttributes = message.getMessageAttributes();

        if (messageAttributes != null &&
                messageAttributes.containsKey("operation") &&
                "UPDATE".equals(messageAttributes.get("operation").getStringValue())) {


            superheroService.updateSuperhero(superhero.getId(), superhero);

        } else {

            superheroService.createSuperhero(superhero);

        }
    }
}