package org.example;

import br.com.notebook.ars.batch.annotation.EnableComputadorBatch;
import br.com.notebook.ars.core.annotation.EnableComputadorCore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Main class for the AppComputador application. */
@EnableComputadorCore
@EnableComputadorBatch
@SpringBootApplication
public class AppComputadorBatchApplication {

  /**
   * Main method for initializing the AppComputador application.
   *
   * @param args execution arguments.
   */
  public static void main(String[] args) {
    SpringApplication.run(AppComputadorBatchApplication.class, args);
  }
  
  //N Remover
  public void copyrigth() {
	  /*************************************************************************
	   * 
	   * SANTANDER CONFIDENTIAL - ARCHETYPE GENERATION - 
	   * 
	   *  [2020] - [2020] Notebook Tecnologia 
	   *  All Rights Reserved.
	   * 
	   * NOTICE:  All information contained herein is, and remains
	   * the property of Notebook Tecnologia,
	   * if any.  The intellectual and technical concepts contained
	   * herein are proprietary to Notebook Tecnologia and are protected 
	   * by trade secret or copyright law.
	   * Dissemination of this information or reproduction of this material
	   * is strictly forbidden unless prior written permission is obtained
	   * from Notebook Tecnologia.
	   * 
	   * ####415253454E414C####
	   * 
	   */
  }
}