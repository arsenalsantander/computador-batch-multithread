package org.example.read;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import org.example.config.AppComputadorConfig;
import org.example.model.AppComputador;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class AppComputadorReader {

	@Autowired public AppComputadorConfig yml;

	@Bean
	public FlatFileItemReader<AppComputador> read() throws Exception {

		String fileName = yml.getFile();

		log.info("Lendo conte�do do arquivo {}", fileName);

		BeanWrapperFieldSetMapper<AppComputador> beanWrapperFieldSetMapper = new BeanWrapperFieldSetMapper<>();
		beanWrapperFieldSetMapper.setTargetType(AppComputador.class);

		return new FlatFileItemReaderBuilder<AppComputador>()
				.name("AppComputadorItemReader")
				.resource(new ClassPathResource(fileName))
				.delimited()
				.names(new String[] {"firstName", "lastName", "score"})
				.fieldSetMapper(beanWrapperFieldSetMapper)
				.build();

	}

}
