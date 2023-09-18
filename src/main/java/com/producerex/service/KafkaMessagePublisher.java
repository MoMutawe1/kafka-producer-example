package com.producerex.service;

import com.producerex.dto.Customer;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;
@Service
public class KafkaMessagePublisher {

    /*
        KafkaTemplate is a class given by Spring framework used to allow producer to talk to Kafka broker.
        template.send  method used to pass a message to a kafka broker topic (take 2 arguments topic name & message).
        template.send  return type is CompletableFuture

        CompletableFuture is Asynchronous (Send and forget)
        But if you want to block the "sending thread" and make it wait until it get the result about the sent messages then you can do:
        future.get() API call from the CompletableFuture.. by doing this the thread will wait for the result to come Synchronous call.
        but definitely it will slow down the producer.. it's better to handle the results Asynchronously so that the subsequent messages
        do not wait for the result of the previous messages.. now how can we do that ?? we can do this using a callback implementation
        using future.whenComplete method in the below example:
    */
    @Autowired
    private KafkaTemplate<String,Object> template;

    public void sendEventsToTopic(Customer customer) {
        try {
            CompletableFuture<SendResult<String, Object>> future = template.send("CustomerEvents", customer);
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    System.out.println("Sent message=[" + customer.toString() +
                            "] with offset=[" + result.getRecordMetadata().offset() + "]");
                } else {
                    System.out.println("Unable to send message=[" +
                            customer.toString() + "] due to : " + ex.getMessage());
                }
            });

        } catch (Exception ex) {
            System.out.println("ERROR : "+ ex.getMessage());
        }
    }

/*    public void sendMessageToTopic(String message){
        CompletableFuture<SendResult<String, Object>> future = template.send("NewTopic3", message); // "KB-Customer-Topic1"
        future.whenComplete((result,ex)->{
            // for success scenario (when exception ex is null)
            if (ex == null) {
                System.out.println("Sent message=[" + message +
                        "] with partition=[" + result.getRecordMetadata().partition() + "]" +
                        " with offset=[" + result.getRecordMetadata().offset() + "]");
                // for failure scenario (when exception ex is not null)
            } else {
                System.out.println("Unable to send message=[" +
                        message + "] due to : " + ex.getMessage());
            }
        });
    }*/
}
