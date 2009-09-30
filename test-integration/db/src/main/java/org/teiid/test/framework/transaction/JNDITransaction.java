/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.transaction;

import javax.naming.InitialContext;
import javax.transaction.UserTransaction;

import org.teiid.test.framework.ConfigPropertyLoader;
import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.TransactionQueryTest;
import org.teiid.test.framework.ConfigPropertyNames.CONNECTION_STRATEGY_PROPS;
import org.teiid.test.framework.exception.TransactionRuntimeException;




public class JNDITransaction extends TransactionContainer {
    UserTransaction userTxn = null;
    
    public JNDITransaction(ConfigPropertyLoader config) {
        super(config);
    }
    
    protected void before(TransactionQueryTest test) {
        if (this.props.getProperty(CONNECTION_STRATEGY_PROPS.JNDINAME_USERTXN) == null) {
            throw new TransactionRuntimeException("No JNDI name found for the User Transaction to look up in application server");
        }

        try {          
 
            // begin the transaction
            InitialContext ctx = new InitialContext();
            this.userTxn = (UserTransaction)ctx.lookup(this.props.getProperty(CONNECTION_STRATEGY_PROPS.JNDINAME_USERTXN));
            this.userTxn.begin();
        } catch (Exception e) {
            throw new TransactionRuntimeException(e);
        }        
    }
    
    protected void after(TransactionQueryTest test) {
        try {
            if (this.userTxn != null) {
                if (test.rollbackAllways()|| test.exceptionOccurred()) {
                    this.userTxn.rollback();
                }
                else {
                    this.userTxn.commit();
                }
                this.userTxn = null;
            }
        } catch (Exception e) {
            throw new TransactionRuntimeException(e);            
        }
    }    
}
