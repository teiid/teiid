/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.transaction;


import java.sql.SQLException;

import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.TransactionQueryTestCase;
import org.teiid.test.framework.ConfigPropertyNames.CONNECTION_STRATEGY_PROPS;
import org.teiid.test.framework.ConfigPropertyNames.TXN_AUTO_WRAP_OPTIONS;
import org.teiid.test.framework.exception.TransactionRuntimeException;


/** 
 * A transaction which is user controlled. 
 */
@SuppressWarnings("nls")
public class LocalTransaction extends TransactionContainer {

    public LocalTransaction() {
	super();
    }
    protected void before(TransactionQueryTestCase test) {
	test.getConnectionStrategy().setEnvironmentProperty(CONNECTION_STRATEGY_PROPS.TXN_AUTO_WRAP, TXN_AUTO_WRAP_OPTIONS.AUTO_WRAP_OFF);
	
        try {
       		debug("Autocommit: " + test.getConnectionStrategy().getAutocommit());
            test.getConnection().setAutoCommit(test.getConnectionStrategy().getAutocommit());
        } catch (SQLException e) {
            throw new TransactionRuntimeException(e);
        }        
    }
    
    protected void after(TransactionQueryTestCase test) {
    	boolean exception = false;
        try {            
            if (test.rollbackAllways()|| test.exceptionOccurred()) {
                test.getConnection().rollback();
                
            }
            else {
                 test.getConnection().commit();
            }
        } catch (SQLException se) {
             se.printStackTrace();
        	exception =  true;
        	// if exception, try to trigger the rollback
        	try {
        		test.getConnection().rollback();
        	} catch (Exception e) {
        		// do nothing
        	}
            throw new TransactionRuntimeException(se);
            
            
        } finally {
        	// if an exception occurs and the autocommit is set to true - while doing a transaction
        	// will generate a new exception overriding the first exception
        	if (!exception) {
	            try {
	                test.getConnection().setAutoCommit(true);
	            } catch (SQLException e) {
	                throw new TransactionRuntimeException(e);
	            }
        	}
        }
    }   
    


}
