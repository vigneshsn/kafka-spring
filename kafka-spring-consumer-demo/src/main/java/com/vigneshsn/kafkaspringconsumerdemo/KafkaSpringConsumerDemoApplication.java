package com.vigneshsn.kafkaspringconsumerdemo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@SpringBootApplication
@Slf4j
public class KafkaSpringConsumerDemoApplication implements ApplicationRunner {

	String bootstrapAddress = "192.168.99.100:29092,192.168.99.100:39092";
	public static void main(String[] args) {
		SpringApplication.run(KafkaSpringConsumerDemoApplication.class, args);
	}

//	@Bean
//	public ConsumerFactory<String, String> consumerFactory() {
//		Map<String, Object> configProps = new HashMap<>();
//		configProps.put(
//				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
//		configProps.put(
//				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
//				StringDeserializer.class);
//		configProps.put(
//				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
//				StringDeserializer.class);
//		return new DefaultKafkaConsumerFactory<>(configProps);
//	}
//
//	// Creating a Listener
//	@Bean
//	public ConcurrentKafkaListenerContainerFactory concurrentKafkaListenerContainerFactory()
//	{
//		ConcurrentKafkaListenerContainerFactory<
//				String, String> factory
//				= new ConcurrentKafkaListenerContainerFactory<>();
//		factory.setConsumerFactory(consumerFactory());
//		return factory;
//	}

	@Override
	public void run(ApplicationArguments args) throws Exception {

		//basic implementation
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", bootstrapAddress );
		properties.setProperty("group.id", "demogroup" );
		properties.setProperty("enable.auto.commit", "false" );
		properties.setProperty("auto.offset.reset", "earliest" );
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer(properties);

		kafkaConsumer.subscribe(Arrays.asList("ordertopic"));

		try {
			while(true) {
				ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
				records.forEach(s -> log.info("shipping order for id {}, status {} ", s.key() , s.value()));
				//Thread.sleep(6000);
				kafkaConsumer.commitSync(Duration.ofMillis(2000));
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			kafkaConsumer.close();
		}

	}
}

//@Component
//@Slf4j
//class ShippingService {
//	@KafkaListener(topics = "ordertopic", groupId = "shippingclient", containerFactory="concurrentKafkaListenerContainerFactory")
//	public void doOnOrder(ConsumerRecord consumerRecord) {
//		log.info("shipping order for id {}, status {} ", consumerRecord.key() , consumerRecord.value());
//	}
//}
