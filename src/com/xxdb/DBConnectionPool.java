package com.xxdb;

import java.util.List;

public interface DBConnectionPool {
	void execute(List<DBTask> tasks);
	void execute(DBTask task);
	int getConnectionCount();
	void shutdown();
	void waitForThreadCompletion();
}
