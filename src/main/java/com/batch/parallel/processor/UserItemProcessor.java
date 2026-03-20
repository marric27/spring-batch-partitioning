package com.batch.parallel.processor;

import org.springframework.batch.item.ItemProcessor;

import com.batch.parallel.entities.User;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UserItemProcessor implements ItemProcessor<User, User>{

    @Override
    public User process(User user) throws Exception {
        final String firstName = user.getFirstName().toUpperCase();
        // final String lastName = user.getLastName().toUpperCase();

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

        log.info("User to UpperCase : " + userUpperCase.getFirstName());

        return userUpperCase;
    }
    
}
