/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.transaction;

import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.exception.TransactionRuntimeException;
import org.teiid.test.framework.connection.ConnectionStrategy;
import org.teiid.test.framework.connection.ConnectionStrategyFactory;



public class TransactionFactory {
	
	public static final String LOCAL_TRANSACTION = "local";     //$NON-NLS-1$
	public static final String XATRANSACTION = "xa"; //$NON-NLS-1$
	public static final String JNDI_TRANSACTION = "jndi"; //$NON-NLS-1$
//	public static final String OFFWRAP_TRANSACTION = "offwrap"; //$NON-NLS-1$
//	public static final String ONWRAP_TRANSACTION = "onwrap"; //$NON-NLS-1$


	
	/**
	 * Transaction Type indicates the type of transaction container to use
	 */
    public static final String TRANSACTION_TYPE = "transaction-type"; //$NON-NLS-1$
//    public static final String AUTOCOMMIT = "autocommit"; //$NON-NLS-1$

        
    private TransactionFactory(){}
        
    
    public static TransactionContainer create()  {
    	TransactionContainer transacton = null;
    	
    	ConnectionStrategy connstrategy = ConnectionStrategyFactory.getInstance().getConnectionStrategy();
             
    	
        String type = connstrategy.getEnvironment().getProperty(TRANSACTION_TYPE, LOCAL_TRANSACTION);
        if (type == null) {
        	throw new RuntimeException("Property " + TRANSACTION_TYPE + " was specified");
        }
        
        if (type.equalsIgnoreCase(LOCAL_TRANSACTION)) {
        	transacton = new LocalTransaction(connstrategy);
        }
        else if (type.equalsIgnoreCase(XATRANSACTION)) {
        	transacton = new XATransaction(connstrategy);
        }
        else if (type.equalsIgnoreCase(JNDI_TRANSACTION)) {
        	transacton = new JNDITransaction(connstrategy);

        } else {
        	new TransactionRuntimeException("Invalid property value of " + type + " for " + TRANSACTION_TYPE );
        }
        
        return transacton;
    }
    
}
