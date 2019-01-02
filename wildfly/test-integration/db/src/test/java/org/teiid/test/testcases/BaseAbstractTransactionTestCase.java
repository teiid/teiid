package org.teiid.test.testcases;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.teiid.test.framework.ConfigPropertyLoader;
import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.datasource.DataSourceMgr;

public abstract class BaseAbstractTransactionTestCase {
 
        
    protected abstract TransactionContainer getTransactionContainter();
    
    
    @BeforeClass
    public static void beforeAll() throws Exception {
	ConfigPropertyLoader.reset();
	ConfigPropertyLoader.getInstance();
    }

    @Before
    public void beforeEach() throws Exception {

    }

    @After
    public void afterEach() throws Exception {

    }
    
    protected void addProperty(String key, String value) {
	
	ConfigPropertyLoader.getInstance().setProperty(key, value);
	
    }
    
    @AfterClass
    public static void afterAll() {	
	DataSourceMgr.getInstance().shutdown();
    }
    
    


}
