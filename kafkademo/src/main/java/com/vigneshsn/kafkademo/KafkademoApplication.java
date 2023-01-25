package com.vigneshsn.kafkademo;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class KafkademoApplication {

	String bootstrapAddress = "192.168.99.100:29092,192.168.99.100:39092";
	public static void main(String[] args) {
		SpringApplication.run(KafkademoApplication.class, args);
	}

//	@Bean
//	KafkaProducer<String,String> kafkaProducer() {
//		Properties props = new Properties();
//		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
//		props.put(
//				ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
//				StringSerializer.class);
//		props.put(
//				ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
//				StringSerializer.class);
//		return new KafkaProducer(props);
//	}
//
//
//	@Bean
//	public KafkaAdmin kafkaAdmin() {
//		Map<String, Object> configs = new HashMap<>();
//		configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
//		return new KafkaAdmin(configs);
//	}

	@Bean
	public ProducerFactory<String, String> producerFactory() {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(
				ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				bootstrapAddress);
		configProps.put(
				ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class);
		configProps.put(
				ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class);
		return new DefaultKafkaProducerFactory<>(configProps);
	}

	@Bean
	public KafkaTemplate<String, String> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

}

@RestController
@Slf4j
@RequiredArgsConstructor
class OrderController {

	//private final KafkaProducer<String, String> kafkaProducer;
	// abstraction for kafka producer and consumer
	private final KafkaTemplate kafkaTemplate;

	@PostMapping("/order")
	public void sendOrder(@RequestBody Order order) {
		ProducerRecord<String, String> record = new ProducerRecord<>("ordertopic", order.getId(), order.getStatus());
		try {
			kafkaTemplate.send(record).get();
			log.info("order status send to kafka successfully");
		} catch (Exception ex) {
			ex.printStackTrace();;
		} finally {
		}
	}
}

@Data
class Order {
	String id;
	String status;
}
