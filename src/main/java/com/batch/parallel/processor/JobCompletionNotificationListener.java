package com.batch.parallel.processor;

import java.time.Duration;
import java.time.LocalDateTime;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class JobCompletionNotificationListener implements JobExecutionListener {

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info(">>> INIZIO JOB: {}", jobExecution.getJobInstance().getJobName());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            LocalDateTime startTime = jobExecution.getStartTime();
            LocalDateTime endTime = jobExecution.getEndTime();

            // Calcolo della durata
            Duration duration = Duration.between(startTime, endTime);

            log.info("!!! JOB COMPLETATO CON SUCCESSO !!!");
            log.info("Tempo totale di esecuzione: {} secondi ({} ms)",
                    duration.getSeconds(),
                    duration.toMillis());
            log.info("Record processati: {}",
                    jobExecution.getStepExecutions().stream()
                            .mapToLong(se -> se.getWriteCount())
                            .sum());
        } else {
            log.error("!!! JOB FALLITO con stato: {}", jobExecution.getStatus());
        }
    }
}