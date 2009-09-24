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
	
//	public static final Connection getSource(String identifier)
//			throws QueryTestFailedException {
//		if (identifier != null) {
//			String mappedName = ConfigPropertyLoader.getProperty(identifier);
//
//			if (mappedName == null) {
//				throw new TransactionRuntimeException("Identifier mapping "
//						+ identifier
//						+ " is not defined in the config properties file");
//			}
//
//			Properties sourceProps;
//			try {
//				sourceProps = DataSourceMgr.getInstance()
//						.getDatasourceProperties(mappedName, identifier);
//			} catch (QueryTestFailedException e) {
//				throw new TransactionRuntimeException(e);
//			}
//
//			if (sourceProps == null) {
//				throw new TransactionRuntimeException("Identifier "
//						+ identifier + " mapped to " + mappedName
//						+ " has no datasource properties");
//			}
//
//			return ConnectionStrategyFactory.getInstance().createDriverStrategy(identifier,
//					sourceProps).getConnection();
//
//		}
//		throw new RuntimeException("No Connection by name :" + identifier); //$NON-NLS-1$
//	}
//
//	public static final XAConnection getXASource(String identifier)
//			throws QueryTestFailedException {
//		if (identifier != null) {
//			String mappedName = ConfigPropertyLoader.getProperty(identifier);
//
//			if (mappedName == null) {
//				throw new TransactionRuntimeException("Identifier mapping "
//						+ identifier
//						+ " is not defined in the config properties file");
//			}
//
//			Properties sourceProps;
//			try {
//				sourceProps = DataSourceMgr.getInstance()
//						.getDatasourceProperties(mappedName, identifier);
//			} catch (QueryTestFailedException e) {
//				throw new TransactionRuntimeException(e);
//			}
//
//			if (sourceProps == null) {
//				throw new TransactionRuntimeException("Identifier "
//						+ identifier + " mapped to " + mappedName
//						+ " has no datasource properties");
//			}
//
//			return ConnectionStrategyFactory.getInstance().createDataSourceStrategy(
//					identifier, sourceProps).getXAConnection();
//		}
//		throw new RuntimeException("No Connection by name :" + identifier); //$NON-NLS-1$
//	}

}
