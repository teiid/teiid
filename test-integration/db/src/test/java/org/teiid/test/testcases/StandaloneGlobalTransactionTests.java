/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.testcases;

import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.teiid.test.framework.ConfigPropertyNames;
import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.transaction.StandaloneGlobalTransaction;


/** 
 * This is global transaction test to be used when transaction is external
 * in places like inside app server
 */
@SuppressWarnings("nls")
public class StandaloneGlobalTransactionTests extends LocalTransactionTests {
    
    private static Properties SYS_PROPS;
    
    @BeforeClass
    public static void beforeAll() throws Exception {
	SYS_PROPS = (Properties) System.getProperties().clone();
	
	System.setProperty(ConfigPropertyNames.CONFIG_FILE, "xa-config.properties");

    }
    
    @Override
    @Before
    public void beforeEach() throws Exception {
	super.beforeEach();
    }
    
    @Override
    @After
    public void afterEach() throws Exception {
	super.afterEach();

    }

    
    @Override
    protected TransactionContainer getTransactionContainter() {

	return new StandaloneGlobalTransaction();
    }
    
    
    @AfterClass
    public static void afterAll() {
	
	System.setProperties(SYS_PROPS);

    }
    
    
    

}
