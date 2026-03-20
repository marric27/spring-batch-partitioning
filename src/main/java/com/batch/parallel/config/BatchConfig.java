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
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import com.batch.parallel.entities.User;
import com.batch.parallel.partitioner.CustomPartitioner;
import com.batch.parallel.processor.UserItemProcessor;

@Configuration
public class BatchConfig {

    @Value("${batch.input.file.name}")
    private String inputFileName;

    @Value("${batch.grid.size}")
    private int gridSize;

    @Value("${batch.chunk.size}")
    private int chunkSize;

    @Value("${batch.pool.core-size}")
    private int poolCoreSize;

    @Value("${batch.pool.max-size}")
    private int poolMaxSize;

    @Value("${batch.pool.queue-capacity}")
    private int poolQueueCapacity;

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
                .partitioner("workerStep", partitioner)
                .step(workerStep)
                .partitionHandler(partitionHandler)
                .gridSize(gridSize)
                .build();
    }

    @Bean
    public Partitioner partitioner() {
        return new CustomPartitioner(new ClassPathResource(inputFileName));
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(poolCoreSize);
        executor.setMaxPoolSize(poolMaxSize);
        executor.setQueueCapacity(poolQueueCapacity);
        executor.setThreadNamePrefix("batch-thread-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.initialize();
        return executor;
    }

    @Bean
    public Step workerStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
            ItemReader<User> reader,
            ItemProcessor<User, User> processor,
            ItemWriter<User> writer) {
        return new StepBuilder("workerStep", jobRepository)
                .<User, User>chunk(chunkSize, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public PartitionHandler partitionHandler(Step workerStep) throws Exception {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setStep(workerStep);
        handler.setTaskExecutor(taskExecutor());
        handler.setGridSize(gridSize);
        handler.afterPropertiesSet();
        return handler;
    }

    @Bean
    @StepScope
    public FlatFileItemReader<User> reader(
            @Value("#{stepExecutionContext['fileName']}") String fileName,
            @Value("#{stepExecutionContext['startItem']}") Integer startItem,
            @Value("#{stepExecutionContext['endItem']}") Integer endItem) {
        Resource resource = new ClassPathResource(fileName);

        return new FlatFileItemReaderBuilder<User>()
                .name("userItemReader")
                .resource(resource)
                // .linesToSkip(1)
                .currentItemCount(startItem)
                .maxItemCount(endItem)
                .delimited()
                .names("index", "userId", "firstName", "lastName", "sex", "email", "phone", "dateOfBirth", "jobTitle")
                .targetType(User.class)
                .build();
    }

    @Bean
    @StepScope
    public UserItemProcessor processor(@Value("#{stepExecutionContext['partitionNumber']}") Integer partitionNumber) {
        return new UserItemProcessor(partitionNumber);
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
