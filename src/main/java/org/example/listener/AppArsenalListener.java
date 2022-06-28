package org.example.listener;

import org.example.model.AppComputador;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AppComputadorListener extends JobExecutionListenerSupport {

	private final JdbcTemplate jdbcTemplate;
	
	@Autowired
	public AppComputadorListener(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}
	
	@Override
	public void afterJob(JobExecution jobExecution) {
		if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
			log.info("Job finalizado com sucesso!");
			jdbcTemplate
			.query( 
					"SELECT first_name, last_name, score FROM appcomputador",
					(rs, row) -> new AppComputador(rs.getString(1), rs.getString(2), rs.getInt(3)))
			.forEach(infos -> log.info("Encontrado {} na base de dados.", infos));
		} else if(jobExecution.getStatus() == BatchStatus.FAILED){
			log.error("Job falhou!");
		}
	}
	
}
