/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.testcases;



import java.sql.Connection;

import javax.sql.XAConnection;

import junit.framework.TestCase;

import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.connection.ConnectionUtil;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.transaction.TransactionFactory;

public class BaseAbstractTransactionTestCase extends TestCase  {

	
    public BaseAbstractTransactionTestCase(String name) {
        super(name);
    }
    
    protected TransactionContainer getTransactionContainter() {
    	return TransactionFactory.create(); 
    }


    public Connection getSource(String identifier) throws QueryTestFailedException {
    	return ConnectionUtil.getSource(identifier);
    }    
    
    public XAConnection getXASource(String identifier) throws QueryTestFailedException {
       	return ConnectionUtil.getXASource(identifier);
     }     

}
