package com.xxdb.consumer.datatransferobject;

import com.xxdb.data.Entity;

public interface IMessage {

		String getTopic();
		
		long getOffset();
		
		Entity getEntity(int colIndex);
		
		<T> T getValue(int colIndex);

}
