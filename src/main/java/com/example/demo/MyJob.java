package com.example.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class MyJob {

    @Autowired
    private JobBuilderFactory jobs;

    @Autowired
    private StepBuilderFactory steps;

    Pessoa pessoa1 = new Pessoa("Raissa", 21);
    Pessoa pessoa2 = new Pessoa("Gabriel", 27);
    Pessoa pessoa3 = new Pessoa("Joaquim", 32);


    List<Pessoa> pessoas = new ArrayList<>(Arrays.asList(pessoa1, pessoa2, pessoa3));

    @Bean
    public Job job() {
        return jobs.get("job")
                .start(step1())
                .next(step2())
                .build();
    }

    @Bean
    public ItemReader<Pessoa> itemReader() {
        return new ListItemReader<>(pessoas);
    }

    @Bean
    public ItemWriter<Pessoa> itemWriter() {
        return new ItemWriter<Pessoa>() {

            private StepExecution stepExecution;

            @Override
            public void write(List<? extends Pessoa> pessoas) {
                for (Pessoa item : pessoas) {
                    System.out.println("item = " + item);
                }
                ExecutionContext stepContext = this.stepExecution.getExecutionContext();
                pessoas.forEach(p -> {
                    Pessoa pessoasR = stepContext.containsKey("pessoa") ? (Pessoa) stepContext.get("pessoa") : new Pessoa();
                    stepContext.put("pessoa", pessoasR);
                });



            }

            @BeforeStep
            public void saveStepExecution(StepExecution stepExecution) {
                this.stepExecution = stepExecution;
            }
        };
    }

    @Bean
    public Step step1() {
        return steps.get("step1")
                .<Pessoa, Pessoa>chunk(1)
                .reader(itemReader())
                .writer(itemWriter())
                .listener(promotionListener())
                .build();
    }

    @Bean
    public Step step2() {
        return steps.get("step2")
                .tasklet((contribution, chunkContext) -> {
                    // retrieve the key from the job execution context
                    Pessoa pessoa = (Pessoa) chunkContext.getStepContext().getJobExecutionContext().get("pessoa");
                    System.out.println("In step 2: step 1 wrote " + pessoa + " items");
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    @Bean
    public ExecutionContextPromotionListener promotionListener() {
        ExecutionContextPromotionListener listener = new ExecutionContextPromotionListener();
        listener.setKeys(new String[] {"pessoa"});
        return listener;
    }



    public static void main(String[] args) throws Exception {
        ApplicationContext context = new AnnotationConfigApplicationContext(MyJob.class);
        JobLauncher jobLauncher = context.getBean(JobLauncher.class);
        Job job = context.getBean(Job.class);
        jobLauncher.run(job, new JobParameters());
    }

}

