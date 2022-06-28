package org.example.process;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.example.config.AppComputadorConfig;
import org.example.model.AppComputador;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class AppComputadorProcessor implements ItemProcessor<AppComputador, AppComputador> {

	@Autowired public AppComputadorConfig yml;

	@Override
	public AppComputador process(final AppComputador appcomputador) throws Exception {
		log.info("Processando valores... ");
		AppComputador transformedAppComputador;
	    transformedAppComputador = transformValues(appcomputador);
		return transformedAppComputador;
	}

	private AppComputador transformValues(final AppComputador appcomputador, AppComputador transformedAppComputador) throws Exception{
		final int score = appcomputador.getScore();

		if(yml.getRestartable()) {
			int MIN_SCORE = 3;
			if (score < MIN_SCORE) {
				log.error("Score insuficiente para o registro {}! Ajuste para um valor maior que {}!", appcomputador, MIN_SCORE);
				throw new Exception("Batch Exception!");
			}
		}

		final String firstName = appcomputador.getFirstName().toUpperCase();
		final String lastName = appcomputador.getLastName().toUpperCase();

		return new AppComputador(firstName, lastName, score);
	}
}
