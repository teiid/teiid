package org.teiid.test.framework.connection;

import java.sql.Connection;
import java.util.Map;

import javax.sql.XAConnection;

import org.teiid.test.framework.datasource.DataSource;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;

// identifier should be the model name that is identfied in the config properties
public class ConnectionUtil {
	public static final Connection getConnection(String identifier, Map<String, DataSource> datasources) throws QueryTestFailedException {
		DataSource ds = null;
		if (identifier != null) {
			ds = datasources.get(identifier);
			if (ds == null) {
				throw new TransactionRuntimeException("DataSource is not mapped to Identifier "
						+ identifier);
			}
				
		}
		
		Connection conn = ConnectionStrategyFactory.getInstance().createDriverStrategy(identifier,
				ds.getProperties()).getConnection();
		// force autocommit back to true, just in case the last user didnt
		try {
			conn.setAutoCommit(true);
		} catch (Exception sqle) {
			throw new QueryTestFailedException(sqle);
		}
		
		return conn;

	}
	
	public static final XAConnection getXAConnection(String identifier, Map<String, DataSource> datasources) throws QueryTestFailedException {
		DataSource ds = null;
		if (identifier != null) {
			ds = datasources.get(identifier);
			if (ds == null) {
				throw new TransactionRuntimeException("DataSource is not mapped to Identifier "
						+ identifier);
			}
				
		}

		return ConnectionStrategyFactory.getInstance().createDataSourceStrategy(
				identifier, ds.getProperties()).getXAConnection();

	}

}
