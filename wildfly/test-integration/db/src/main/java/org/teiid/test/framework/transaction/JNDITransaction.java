/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.transaction;

import javax.naming.InitialContext;
import javax.transaction.UserTransaction;

import org.teiid.test.framework.ConfigPropertyNames;
import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.TransactionQueryTestCase;
import org.teiid.test.framework.exception.TransactionRuntimeException;



@SuppressWarnings("nls")
public class JNDITransaction extends TransactionContainer {
    UserTransaction userTxn = null;
    
    
    public JNDITransaction() {
	super();
    }
    
    protected void before(TransactionQueryTestCase test) {
	String jndi = test.getConnectionStrategy().getEnvironment().getProperty(ConfigPropertyNames.CONNECTION_STRATEGY_PROPS.JNDINAME_USERTXN);
	if (jndi == null) {
            throw new TransactionRuntimeException("No JNDI name found for the User Transaction to look up in application server");
        }

        try {          
 
            // begin the transaction
            InitialContext ctx = new InitialContext();
            this.userTxn = (UserTransaction)ctx.lookup(jndi);
             this.userTxn.begin();
        } catch (Exception e) {
            throw new TransactionRuntimeException(e);
        }        
    }
    
    protected void after(TransactionQueryTestCase test) {
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
