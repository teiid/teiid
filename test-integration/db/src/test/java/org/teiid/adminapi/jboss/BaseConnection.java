package org.teiid.adminapi.jboss;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.teiid.jdbc.TeiidDataSource;

@SuppressWarnings("nls")
public class BaseConnection {
	static String user = "admin";
	static String password = "teiid";
	
	interface DataSourceFactory{
		Connection getConnection(String vdbName) throws Exception;
	}
	
	static class ServerDatasourceConnection implements DataSourceFactory {
		public Connection getConnection(String vdbName) throws Exception {
			TeiidDataSource ds = new TeiidDataSource();
			ds.setUser(user);
			ds.setPassword(password);
			ds.setServerName("localhost");
			ds.setPortNumber(31000);
			ds.setDatabaseName(vdbName);
			ds.setAutoCommitTxn("DETECT");
			return ds.getConnection();
		}
	}
	
	public void execute(DataSourceFactory connF, String vdbName, String sql) throws Exception {
		Connection connection = connF.getConnection(vdbName);
		try {
			connection.getMetaData();
			Statement statement = connection.createStatement();
			boolean hasResults = statement.execute(sql);
			if (hasResults) {
				ResultSet results = statement.getResultSet();
				ResultSetMetaData metadata = results.getMetaData();
				int columns = metadata.getColumnCount();
				
				while(results.next()) {
					for (int i = 0; i < columns; i++) {
						System.out.print(results.getString(i+1));
						System.out.print(",");
					}
					System.out.println("");
				}
				System.out.println("Done getting results!");
				results.close();				
			}
			else {
				System.out.println("update count is="+statement.getUpdateCount());
			}
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				connection.close();
			}
		}		
	}	
}
