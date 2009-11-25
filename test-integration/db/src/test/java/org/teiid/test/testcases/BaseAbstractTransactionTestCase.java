package org.teiid.test.testcases;

import junit.framework.TestCase;

import org.teiid.test.framework.ConfigPropertyLoader;
import org.teiid.test.framework.TransactionContainer;

public abstract class BaseAbstractTransactionTestCase extends TestCase {
 
    protected TransactionContainer container = null;

    public BaseAbstractTransactionTestCase(String name) {
	super(name);

	
    }
        
    protected abstract TransactionContainer getTransactionContainter();
    
    

     @Override
    protected void setUp() throws Exception {
	// TODO Auto-generated method stub
	super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
	// TODO Auto-generated method stub
	super.tearDown();

    }


    protected void addProperty(String key, String value) {
	
	ConfigPropertyLoader.getInstance().setProperty(key, value);
	
    }



}
