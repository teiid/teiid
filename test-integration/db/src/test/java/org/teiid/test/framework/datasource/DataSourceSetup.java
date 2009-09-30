package org.teiid.test.framework.datasource;

import java.util.Map;

import org.teiid.test.framework.connection.ConnectionStrategy;
import org.teiid.test.framework.datasource.DataSource;
import org.teiid.test.framework.exception.QueryTestFailedException;

/**
 * The DataSourceSetup 
 * @author vanhalbert
 *
 */
public interface DataSourceSetup {
	
	
	void setup(Map<String, DataSource> datasources, ConnectionStrategy connStrategy) throws QueryTestFailedException;
	

	int getDataSourceCnt();
	
}
