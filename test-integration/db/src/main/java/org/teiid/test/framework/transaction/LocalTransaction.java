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
        
//        this.props.setProperty(ConnectionStrategy.TXN_AUTO_WRAP, ConnectionStrategy.AUTO_WRAP_OFF);
    }
    
    protected void before(TransactionQueryTest test) {
        try {
            test.getConnection().setAutoCommit(this.connStrategy.getAutocommit());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }        
    }
    
    protected void after(TransactionQueryTest test) {
        try {            
            if (test.rollbackAllways()|| test.exceptionOccurred()) {
                test.getConnection().rollback();
                
            }
            else {
                test.getConnection().commit();
            }
        } catch (SQLException e) {
            throw new TransactionRuntimeException(e);
        } finally {
            try {
                test.getConnection().setAutoCommit(true);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }   
}
