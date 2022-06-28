package org.example.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Getter;
import lombok.Setter;

@Configuration
@Getter
@Setter
@ConfigurationProperties(prefix = "computador.model")
public class AppComputadorConfig {

	private String environment;
	private String file;
	private Boolean restartable;

	
}
