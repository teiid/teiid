/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.testcases;



import junit.framework.TestCase;

import org.teiid.test.framework.ConfigPropertyLoader;
import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.transaction.TransactionFactory;

public class BaseAbstractTransactionTestCase extends TestCase  {

	protected ConfigPropertyLoader config = null;
	
    public BaseAbstractTransactionTestCase(String name) {
        super(name);
    }
    

    
    @Override
	protected void setUp() throws Exception {
		// TODO Auto-generated method stub
		super.setUp();
		
		config = ConfigPropertyLoader.createInstance();

	}

    
    protected ConfigPropertyLoader getConfig() {
    	return this.config;
    }

	protected void addProperty(String key, String value) {
    	config.setProperty(key, value);
    }
	
	/**
	 * Call to assign a specific database type to the model.   When a datasource is requested for this model,
	 * a datasource of the specific dbtype will be returned.  See {@link DataSourceFactory} for the logic that
	 * controls this behavior.
	 * @param modelName
	 * @param dbtype
	 *
	 * @since
	 */
	protected void setAssignModelToDatabaseType(String modelName, String dbtype) {
		config.setModelAssignedToDatabaseType(modelName, dbtype);
	}
    
    protected TransactionContainer getTransactionContainter() throws QueryTestFailedException {
    	return TransactionFactory.create(config); 
    }    

}
