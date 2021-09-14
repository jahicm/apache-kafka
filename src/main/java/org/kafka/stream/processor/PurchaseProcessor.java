package org.kafka.stream.processor;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;



public class PurchaseProcessor<String, Purchase>  implements ProcessorSupplier<String,Purchase,String,Purchase>,Processor<String,Purchase,String,Purchase> {

	@Override
	public void process(Record<String, Purchase> record) {
		System.out.println("*********** process method**************"+record.value());
		
		
		
	}

	@Override
	public Processor<String, Purchase, String, Purchase> get() {
		
		return new PurchaseProcessor<String, Purchase>();
	}

	
	
	
}
