
package org.example.write;

import java.util.List;

import javax.sql.DataSource;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.example.model.AppComputador;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class AppComputadorWriter implements ItemWriter<AppComputador> {

	@Autowired
	private DataSource dataSource;

	@Override
	public void write(List<? extends AppComputador> appcomputador) throws Exception {
		log.info("Gravando arquivos no pool: {}", dataSource);
		JdbcBatchItemWriter<AppComputador> writer = new JdbcBatchItemWriterBuilder<AppComputador>()
				.beanMapped()
				.dataSource(this.dataSource)
				.sql("INSERT INTO AppComputador (first_name, last_name, score) VALUES (:firstName, :lastName, :score)")
				.build();
		writer.afterPropertiesSet();
		writer.write(appcomputador);
	}
}