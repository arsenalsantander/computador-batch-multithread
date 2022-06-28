        package org.example.config;

import java.sql.SQLException;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * for For BA Credentials, uncomment imports bellow:
 *
 * import br.com.notebook.ba.credentialsclient.BACredentialsClient;
 * import br.com.notebook.ba.credentialsclient.Credentials;
 * import org.springframework.beans.factory.annotation.Value;
 */
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j;


/**
 * Configuration class for reading secrets and connecting to database.
 */
@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "computador.library.batch.config.database")
@Log4j
public class DatabaseConfig {

  /**
   * Jdbc url for database.
   */
  private String url;

  /**
   * Database driver class name.
   */
  private String driverClassName;

  /**
   * Database user.
   */
  private String username;

  /**
   * Database password.
   */
  private String password;

  /**
   * Database poolName.
   */
  private String poolName;

  /**
   * Database maxPoolSize.
   */
  private Integer maxPoolSize;

  /**
   * Database minPoolSize.
   */
  private Integer minPoolSize;

  /**
   * Database maxLifetime.
   */
  private Integer maxLifetime;

  /**
   * Database validationTimeout.
   */
  private Integer validationTimeout;

    /* Properties to obtain BA Credentials

     * Path to pem file.
    @Value("${directory.pem}")
    private String pathFilePem;

     * Profile of BA Credentials.
     @Value("${directory.profiles}")
    private String profile;

     * System of BA Credentials.
    @Value("${directory.sigla}")
    private String sigla;

     * Index of BA Credentials.
    @Value("${directory.index}")
    private String index;

    */

  /**
   * Creates a DataSource Bean to connect to the database.
   *
   * @return Connection DataSource.
   */
  @Bean
  public DataSource dataSource() throws SQLException  {
    try {
      HikariConfig hikariConfig = new HikariConfig();

            /*
             * Create BA Credentials client and otain Credentials of database

             BACredentialsClient client = new BACredentialsClient(profile, pathFilePem);
             Credentials credentials = client.getCredentials(sigla, index);
             String username = credentials.getUser();
             String password = credentials.getPassword();

             */

      hikariConfig.setDriverClassName(driverClassName);
      hikariConfig.setJdbcUrl(url);
      hikariConfig.setUsername(username);
      hikariConfig.setPassword(password);
      hikariConfig.setPoolName(poolName);
      hikariConfig.setMinimumIdle(minPoolSize);
      hikariConfig.setMaximumPoolSize(maxPoolSize);
      hikariConfig.setMaxLifetime(maxLifetime);
      hikariConfig.setValidationTimeout(validationTimeout);
      return new HikariDataSource(hikariConfig);
    } catch (Exception e) {
      log.error(e);
      throw new SQLException("Falha ao conectar no banco de dados.");
    }
  }

}
