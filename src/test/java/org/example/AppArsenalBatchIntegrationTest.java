package org.example;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
@SpringBatchTest
@ContextConfiguration(classes = {AppComputadorBatchApplication.class})
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
@TestExecutionListeners({
  DependencyInjectionTestExecutionListener.class,
  DirtiesContextTestExecutionListener.class
})
public class AppComputadorBatchIntegrationTest {

  @Autowired 
  public JobLauncherTestUtils jobLauncherTestUtils;

  @Autowired 
  public JobRepositoryTestUtils jobRepositoryTestUtils;

  @Test
  public void howToJobNameAndStatus() throws Exception {
    JobExecution jobExecution = jobLauncherTestUtils.launchJob();
    JobInstance actualJobInstance = jobExecution.getJobInstance();
    ExitStatus actualJobExitStatus = jobExecution.getExitStatus();
    
    assertEquals(actualJobInstance.getJobName(), "appcomputadorJobExec");
    assertEquals(actualJobExitStatus.getExitCode(), "COMPLETED");
  }
}
