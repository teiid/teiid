package org.teiid.test.framework.connection;

import java.sql.Connection;
import java.util.Properties;

import javax.sql.XAConnection;

import org.teiid.test.framework.ConfigPropertyLoader;
import org.teiid.test.framework.datasource.DatasourceMgr;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;

public class ConnectionUtil {
	public static final Connection getSource(String identifier)
			throws QueryTestFailedException {
		if (identifier != null) {
			Properties props = ConfigPropertyLoader
					.loadConfigurationProperties();
			String mappedName = props.getProperty(identifier);

			if (mappedName == null) {
				throw new TransactionRuntimeException("Identifier mapping "
						+ identifier
						+ " is not defined in the config properties file");
			}

			Properties sourceProps;
			try {
				sourceProps = DatasourceMgr.getInstance()
						.getDatasourceProperties(mappedName);
			} catch (QueryTestFailedException e) {
				throw new TransactionRuntimeException(e);
			}

			if (sourceProps == null) {
				throw new TransactionRuntimeException("Identifier "
						+ identifier + " mapped to " + mappedName
						+ " has no datasource properties");
			}

			return ConnectionStrategyFactory.createDriverStrategy(identifier,
					sourceProps).getConnection();

		}
		throw new RuntimeException("No Connection by name :" + identifier); //$NON-NLS-1$
	}

	public static final XAConnection getXASource(String identifier)
			throws QueryTestFailedException {
		if (identifier != null) {
			Properties props = ConfigPropertyLoader
					.loadConfigurationProperties();
			String mappedName = props.getProperty(identifier);

			if (mappedName == null) {
				throw new TransactionRuntimeException("Identifier mapping "
						+ identifier
						+ " is not defined in the config properties file");
			}

			Properties sourceProps;
			try {
				sourceProps = DatasourceMgr.getInstance()
						.getDatasourceProperties(mappedName);
			} catch (QueryTestFailedException e) {
				throw new TransactionRuntimeException(e);
			}

			if (sourceProps == null) {
				throw new TransactionRuntimeException("Identifier "
						+ identifier + " mapped to " + mappedName
						+ " has no datasource properties");
			}

			return ConnectionStrategyFactory.createDataSourceStrategy(
					identifier, sourceProps).getXAConnection();
		}
		throw new RuntimeException("No Connection by name :" + identifier); //$NON-NLS-1$
	}

}
