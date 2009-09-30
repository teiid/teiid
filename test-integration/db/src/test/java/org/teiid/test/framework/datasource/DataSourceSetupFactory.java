package org.teiid.test.framework.datasource;

import org.teiid.test.framework.exception.TransactionRuntimeException;

public class DataSourceSetupFactory {
	
	public static DataSourceSetup createDataSourceSetup(int numofsources)  {
		DataSourceSetup dss = null;
		
		switch (numofsources) {
		case 1:
			dss = new SingleDataSourceSetup();
			break;
			
		case 2:
			dss = new TwoDataSourceSetup();
			break;
			
		default:
			throw new TransactionRuntimeException("Number of datasources " + numofsources + " is not supported");

		}
		
		return dss;
	}

}
