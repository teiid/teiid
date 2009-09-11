package org.teiid.test.framework.datasource;

import org.teiid.test.framework.DataSourceSetup;
import org.teiid.test.framework.exception.QueryTestFailedException;

public class DataSourceSetupFactory {
	
	public static DataSourceSetup createDataSourceSetup(int numOfDataSources) throws QueryTestFailedException {
		DataSourceSetup dss = null;
		
		switch (numOfDataSources) {
		case 1:
			dss = new SingleDataSourceSetup();
			break;
			
		case 2:
			dss = new TwoDataSourceSetup();
			break;
			
		default:
			throw new QueryTestFailedException("Number of datasources " + numOfDataSources + " is not supported");

		}
		
		return dss;
	}

}
