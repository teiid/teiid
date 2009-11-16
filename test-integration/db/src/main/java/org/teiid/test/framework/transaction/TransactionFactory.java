/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.transaction;

import org.teiid.test.framework.ConfigPropertyLoader;
import org.teiid.test.framework.ConfigPropertyNames;
import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;



public class TransactionFactory {
	

        
    private TransactionFactory(){}
        
    
    public static TransactionContainer create(ConfigPropertyLoader config) throws QueryTestFailedException {
    	TransactionContainer transacton = null;
    	    	
//        String type = config.getProperty(ConfigPropertyNames.TRANSACTION_TYPE);
//        if (type == null) {
//        	type = ConfigPropertyNames.TRANSACTION_TYPES.LOCAL_TRANSACTION;
//        }
//        
//        System.out.println("Create TransactionContainer: " + type);
//        
//        if (type.equalsIgnoreCase(ConfigPropertyNames.TRANSACTION_TYPES.LOCAL_TRANSACTION)) {
//        	transacton = new LocalTransaction(config);
//        }
//        else if (type.equalsIgnoreCase(ConfigPropertyNames.TRANSACTION_TYPES.XATRANSACTION)) {
//        	transacton = new XATransaction(config);
//        }
//        else if (type.equalsIgnoreCase(ConfigPropertyNames.TRANSACTION_TYPES.JNDI_TRANSACTION)) {
//        	transacton = new JNDITransaction(config);
//
//        } else {
//        	throw new TransactionRuntimeException("Invalid property value of " + type + " for " + ConfigPropertyNames.TRANSACTION_TYPE );
//        }

        return transacton;
    }
    
}
