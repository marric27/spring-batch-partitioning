package com.batch.parallel.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.batch.parallel.entities.User;
import com.batch.parallel.partitioner.CustomPartitioner;
import com.batch.parallel.processor.UserItemProcessor;

@Configuration
public class BatchConfig {

    @Bean
    public Job partitionedJob(JobRepository jobRepository, Step partitionStep) {
        return new JobBuilder("partitionedJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(partitionStep)
                .build();
    }

    @Bean
    public Step partitionStep(JobRepository jobRepository, Step workerStep, Partitioner partitioner,
            PartitionHandler partitionHandler) {
        return new StepBuilder("partitionStep", jobRepository)
                .partitioner("workerStep", partitioner) // Il nome deve corrispondere al bean dello step "lavoratore"
                .step(workerStep)
                .partitionHandler(partitionHandler)
                .build();
    }

    @Bean
    public Partitioner partitioner() {
        return new CustomPartitioner();
    }

    @Bean
    public Step workerStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
            ItemReader<User> reader,
            ItemProcessor<User, User> processor,
            ItemWriter<User> writer) {
        return new StepBuilder("workerStep", jobRepository)
                .<User, User>chunk(10, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public PartitionHandler partitionHandler(Step workerStep) {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setStep(workerStep);
        handler.setTaskExecutor(new SimpleAsyncTaskExecutor()); // Abilita il parallelismo
        handler.setGridSize(4); // Numero di partizioni da gestire contemporaneamente
        return handler;
    }

    @Bean
    public FlatFileItemReader<User> reader() {
        return new FlatFileItemReaderBuilder<User>()
                .name("userItemReader")
                .resource(new ClassPathResource("people-10000.csv"))
                .delimited()
                .names("index", "userId", "firstName", "lastName", "sex", "email", "phone", "dateOfBirth", "jobTitle")
                .targetType(User.class)
                .build();
    }

    @Bean
    public UserItemProcessor processor() {
        return new UserItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<User> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<User>()
                .dataSource(dataSource)
                .sql("INSERT INTO users (user_id, first_name, last_name, sex, email, phone, date_of_birth, job_title) "
                        +
                        "VALUES (:userId, :firstName, :lastName, :sex, :email, :phone, :dateOfBirth, :jobTitle)")
                .beanMapped()
                .build();
    }

}