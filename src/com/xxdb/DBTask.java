package com.xxdb;

import java.util.concurrent.Callable;
import com.xxdb.data.Entity;

public interface DBTask extends Callable<Entity>{
	void setDBConnection(DBConnection conn);
	Entity getResult();
	String getErrorMsg();
	boolean isSuccessful();
	String getScript();
}
