/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.transaction;

import java.sql.SQLException;

import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.TransactionQueryTest;
import org.teiid.test.framework.exception.TransactionRuntimeException;
import org.teiid.test.framework.connection.ConnectionStrategy;



/** 
 * A transaction which is user controlled. 
 */
public class LocalTransaction extends TransactionContainer {

    public LocalTransaction(ConnectionStrategy strategy) {
        super(strategy);
        
    }
    
    protected void before(TransactionQueryTest test) {
        try {
        	debug("Autocommit: " + this.connStrategy.getAutocommit());
            test.getConnection().setAutoCommit(this.connStrategy.getAutocommit());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }        
    }
    
    protected void after(TransactionQueryTest test) {
    	boolean exception = false;
        try {            
            if (test.rollbackAllways()|| test.exceptionOccurred()) {
                test.getConnection().rollback();
                
            }
            else {
                test.getConnection().commit();
            }
        } catch (SQLException se) {
        	exception =  true;
        	// if exception, try to trigger the rollback
        	try {
        		test.getConnection().rollback();
        	} catch (Exception e) {
        		// do nothing
        	}
            throw new TransactionRuntimeException(se);
            
            
        } finally {
        	// if an exceptio occurs and the autocommit is set to true - while doing a transaction
        	// will generate a new exception overriding the first exception
        	if (!exception) {
	            try {
	                test.getConnection().setAutoCommit(true);
	            } catch (SQLException e) {
	                throw new RuntimeException(e);
	            }
        	}
        }
    }   
}
