package com.example.superhero_localstack.service;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.example.superhero_localstack.DTO.SuperheroMessage;
import com.example.superhero_localstack.model.Superhero;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.aws.messaging.listener.SqsMessageDeletionPolicy;
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class SuperheroMessageConsumer {

    private static final String QUEUE_URL = "http://localhost:4566/000000000000/superhero-queue";

    @Autowired
    private AmazonSQS amazonSQS;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private SuperheroService superheroService;

    public void SuperheroMessageListener(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @SqsListener(value = "${aws.sqs.queue.name}", deletionPolicy = SqsMessageDeletionPolicy.ON_SUCCESS)
    public void receiveMessages(String message) {
        try {

            SuperheroMessage superheroMessage = objectMapper.readValue(
                    message,
                    SuperheroMessage.class
            );
            processMessage(superheroMessage);

        } catch (Exception e) {
            throw new RuntimeException("Error processing message", e);
        }
    }


    //    @Scheduled(fixedDelay = 30000)
//    public void receiveMessages() {
//        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
//                .withQueueUrl(QUEUE_URL)
//                .withMaxNumberOfMessages(10)
//                .withWaitTimeSeconds(20);
//
//        List<Message> messages = amazonSQS.receiveMessage(receiveMessageRequest).getMessages();
//
//        for (Message message : messages) {
//            try {
//                SuperheroMessage superheroMessage = objectMapper.readValue(
//                        message.getBody(),
//                        SuperheroMessage.class
//                );
//                processMessage(superheroMessage);
//
//                amazonSQS.deleteMessage(QUEUE_URL, message.getReceiptHandle());
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//    }

    private void processMessage(SuperheroMessage message) {
        switch (message.getOperation()) {
            case "CREATE":
                Superhero newHero = new Superhero();
                newHero.setName(message.getName());
                newHero.setPower(message.getPower());
                newHero.setUniverse(message.getUniverse());
                newHero.setArchEnemy(message.getArchEnemy());
                superheroService.createSuperhero(newHero);
                break;

            case "UPDATE":
                Superhero updateHero = new Superhero();
                updateHero.setName(message.getName());
                updateHero.setPower(message.getPower());
                updateHero.setUniverse(message.getUniverse());
                updateHero.setArchEnemy(message.getArchEnemy());
                superheroService.updateSuperhero(message.getSuperheroId(), updateHero);
                break;

            case "DELETE":
                superheroService.deleteSuperhero(message.getSuperheroId());
                break;

            default:
                throw new IllegalArgumentException("Unknown operation: " + message.getOperation());
        }
    }
}