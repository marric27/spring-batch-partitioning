package com.batch.parallel.processor;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Comparator;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.StepExecution;
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

            Duration duration = Duration.between(startTime, endTime);

            log.info("!!! JOB COMPLETATO CON SUCCESSO !!!");
            log.info("Tempo totale di esecuzione: {} secondi ({} ms)",
                    duration.getSeconds(),
                    duration.toMillis());
            log.info("Record processati: {}",
                    jobExecution.getStepExecutions().stream()
                            .mapToLong(StepExecution::getWriteCount)
                            .sum());

            jobExecution.getStepExecutions().stream()
                    .sorted(Comparator.comparing(StepExecution::getStepName))
                    .forEach(this::logStepStatistics);
        } else {
            log.error("!!! JOB FALLITO con stato: {}", jobExecution.getStatus());
        }
    }

    private void logStepStatistics(StepExecution stepExecution) {
        Integer partitionNumber = stepExecution.getExecutionContext().containsKey("partitionNumber")
                ? stepExecution.getExecutionContext().getInt("partitionNumber")
                : null;

        log.info(
                "Step stats - stepName={} partitionNumber={} readCount={} writeCount={} skipCount={}",
                stepExecution.getStepName(),
                partitionNumber,
                stepExecution.getReadCount(),
                stepExecution.getWriteCount(),
                stepExecution.getSkipCount());
    }
}
