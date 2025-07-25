package com.xxdb;

import java.util.concurrent.Callable;
import com.xxdb.data.Entity;

public interface DBTask extends Callable<Entity>{
	void setDBConnection(DBConnection conn);
	void setDBConnection(DBConnection conn, boolean enableSeqNo);
	Entity getResult();
	String getErrorMsg();
	String getScript();
	boolean isSuccessful();
	boolean isFinished();
}
