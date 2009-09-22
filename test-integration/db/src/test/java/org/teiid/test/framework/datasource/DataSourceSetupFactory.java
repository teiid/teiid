package org.teiid.test.framework.datasource;

import java.util.Map;

import org.teiid.test.framework.DataSourceSetup;
import org.teiid.test.framework.exception.QueryTestFailedException;

public class DataSourceSetupFactory {
	
	public static DataSourceSetup createDataSourceSetup(Map<String, DataSource> datasources) throws QueryTestFailedException {
		DataSourceSetup dss = null;
		
		switch (datasources.size()) {
		case 1:
			dss = new SingleDataSourceSetup(datasources);
			break;
			
		case 2:
			dss = new TwoDataSourceSetup(datasources);
			break;
			
		default:
			throw new QueryTestFailedException("Number of datasources " + datasources.size() + " is not supported");

		}
		
		return dss;
	}

}
