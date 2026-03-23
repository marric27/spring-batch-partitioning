package com.batch.parallel.config;

import javax.sql.DataSource;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
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
    public Job asyncJob(JobRepository jobRepository, Step workerStep) {
        return new JobBuilder("asyncJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(workerStep) // Non c've più il partitionStep
                .build();
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
    public Step workerStep(JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            ItemReader<User> reader,
            AsyncItemProcessor<User, User> asyncProcessor,
            AsyncItemWriter<User> asyncWriter) {
        return new StepBuilder("workerStep", jobRepository).<User, java.util.concurrent.Future<User>>chunk(chunkSize,
                transactionManager)
                .reader(reader)
                .processor(asyncProcessor)
                .writer(asyncWriter)
                // .taskExecutor(taskExecutor())
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        // puoi lanciare thread indipendenti
                        new Thread(() -> {
                            while (true) {
                                System.out.println("Azione in background...");
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                        }).start();
                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        return null;
                    }
                })
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<User> databaseWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<User>()
                .dataSource(dataSource)
                .sql("INSERT INTO users (user_id, first_name, last_name, sex, email, phone, date_of_birth, job_title) "
                        + "VALUES (:userId, :firstName, :lastName, :sex, :email, :phone, :dateOfBirth, :jobTitle)")
                .beanMapped()
                .build();
    }

    @Bean
    public UserItemProcessor delegateProcessor() {
        return new UserItemProcessor();
    }

    @Bean
    public AsyncItemProcessor<User, User> asyncProcessor(UserItemProcessor delegateProcessor) {
        AsyncItemProcessor<User, User> asyncProcessor = new AsyncItemProcessor<>();
        asyncProcessor.setDelegate(delegateProcessor);
        asyncProcessor.setTaskExecutor(taskExecutor()); // Usa il pool di thread
        return asyncProcessor;
    }

    @Bean
    public AsyncItemWriter<User> asyncWriter(JdbcBatchItemWriter<User> delegateWriter) {
        AsyncItemWriter<User> asyncWriter = new AsyncItemWriter<>();
        asyncWriter.setDelegate(delegateWriter);
        return asyncWriter;
    }

    // Il Reader torna normale (senza StepScope o parametri di partizione)
    @Bean
    public FlatFileItemReader<User> reader() {
        return new FlatFileItemReaderBuilder<User>()
                .name("userItemReader")
                .resource(new ClassPathResource(inputFileName))
                .delimited()
                .names("index", "userId", "firstName", "lastName", "sex", "email", "phone", "dateOfBirth", "jobTitle")
                .targetType(User.class)
                .build();
    }

}
