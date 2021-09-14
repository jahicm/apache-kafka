package org.kafka.stream;

import org.kafka.stream.four.KafkaStreamFour;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class KafkaApplication {

	public static void main(String[] args) throws Exception {

		SpringApplication.run(KafkaApplication.class, args);
		
		/* KafkaStreamFour ksOne = new KafkaStreamFour(); ksOne.start(); */ 
		
		/* KafkaStreamTwo ksTwo = new KafkaStreamTwo(); ksTwo.start(); */
		
		/* KafkaStreamThree ksThree = new KafkaStreamThree(); ksThree.start(); */
		
		 KafkaStreamFour ksFour = new KafkaStreamFour(); ksFour.start(); 

	
	}
}