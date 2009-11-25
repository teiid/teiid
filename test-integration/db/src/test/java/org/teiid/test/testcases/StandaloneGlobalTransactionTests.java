/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.testcases;

import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.transaction.StandaloneGlobalTransaction;


/** 
 * This is global transaction test to be used when transaction is external
 * in places like inside app server
 */
public class StandaloneGlobalTransactionTests extends LocalTransactionTests {

    public StandaloneGlobalTransactionTests(String testName) {
        super(testName);
    }
    
    @Override
    protected TransactionContainer getTransactionContainter() {

	return new StandaloneGlobalTransaction();
    }
    

}
