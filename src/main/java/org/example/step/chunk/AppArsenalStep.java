package org.example.step.chunk;

import org.example.model.AppComputador;
import org.example.process.AppComputadorProcessor;
import org.example.read.AppComputadorReader;
import org.example.write.AppComputadorWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.batch.item.file.FlatFileParseException;

@Slf4j
@Configuration
public class AppComputadorStep {

	@Autowired public StepBuilderFactory stepBuilderFactory;

	@Autowired public AppComputadorReader reader;

	@Autowired public AppComputadorProcessor processor;

	@Autowired public AppComputadorWriter writer;

	@Bean
	public SimpleAsyncTaskExecutor taskExecutor() {
	  SimpleAsyncTaskExecutor simpleAsyncTaskExecutor = new SimpleAsyncTaskExecutor();
	  simpleAsyncTaskExecutor.setConcurrencyLimit(100);
	  simpleAsyncTaskExecutor.setThreadPriority(0);
	  simpleAsyncTaskExecutor.setThreadNamePrefix("MySimpleAsyncThreads");
	  return simpleAsyncTaskExecutor;
	}
	  
	@Bean
	public Step step() throws Exception {
		log.info("Iniciando step...");
		return stepBuilderFactory
			.get("step")
			.<AppComputador, AppComputador>chunk(100)
			.reader(reader.read())
			.processor(processor)
			.writer(writer)
			.faultTolerant()
			.skipLimit(100)
			.skip(FlatFileParseException.class)
		    .taskExecutor(new SimpleAsyncTaskExecutor())
		    .build();
	}

}
