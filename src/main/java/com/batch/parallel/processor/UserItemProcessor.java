package com.batch.parallel.processor;

import org.springframework.batch.item.ItemProcessor;

import com.batch.parallel.entities.User;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UserItemProcessor implements ItemProcessor<User, User> {

    // private final Integer partitionNumber;
    private int count = 0;

    public UserItemProcessor() {
    }

    // public UserItemProcessor(Integer partitionNumber) {
    //     this.partitionNumber = partitionNumber;
    // }

    @Override
    public User process(User user) throws Exception {
        final String firstName = user.getFirstName().toUpperCase();
        final String threadName = Thread.currentThread().getName();

        final User userUpperCase = User.builder()
                .index(user.getIndex())
                .userId(user.getUserId())
                .firstName(firstName)
                .lastName(user.getLastName())
                .sex(user.getSex())
                .email(user.getEmail())
                .phone(user.getPhone())
                .dateOfBirth(user.getDateOfBirth())
                .jobTitle(user.getJobTitle())
                .build();

         count++;
        // Logga ogni 2500 righe per non intasare la console con 1 milione di log
         if (count % 100 == 0) {
        // System.out.println(String.format("[THREAD-PARTITION-%d] Elaborate %d righe.
        // Ultimo ID: %s",
        // partitionNumber, count, user.getUserId()));
        // }

        log.info(
        "Processing user firstName={} on thread={} partitionNumber={}",
        userUpperCase.getFirstName(),
        threadName,0);}
        // partitionNumber);

        return userUpperCase;
    }
}
