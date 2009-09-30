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



	protected void addProperty(String key, String value) {
    	config.setProperty(key, value);
    }
    
    protected TransactionContainer getTransactionContainter() throws QueryTestFailedException {
    	return TransactionFactory.create(config); 
    }    

}
