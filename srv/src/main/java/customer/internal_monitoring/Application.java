package customer.internal_monitoring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

import customer.internal_monitoring.config.KafkaMonitoringProperties;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(KafkaMonitoringProperties.class)
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

}
