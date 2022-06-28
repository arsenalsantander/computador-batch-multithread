package org.example.job;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.example.listener.AppComputadorListener;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@EnableBatchProcessing
public class AppComputadorJob {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Bean
	public Job appcomputadorJobExec(Step step, AppComputadorListener listener) {
		log.info("Iniciando job...");
		
		return jobBuilderFactory.get("appcomputadorJobExec")
				.start(step)
				.listener(listener)
				.build();
	}

}
