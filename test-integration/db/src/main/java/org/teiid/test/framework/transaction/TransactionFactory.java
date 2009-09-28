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
import org.teiid.test.framework.connection.ConnectionStrategy;
import org.teiid.test.framework.connection.ConnectionStrategyFactory;



public class TransactionFactory {
	

        
    private TransactionFactory(){}
        
    
    public static TransactionContainer create() throws QueryTestFailedException {
    	TransactionContainer transacton = null;
    	
    	// load the configuration properties defined at this time for the test
    	ConfigPropertyLoader.loadConfigurationProperties();

    	
 //   	ConnectionStrategy connstrategy = ConnectionStrategyFactory.getInstance().getConnectionStrategy();
    	
//    	Properties props = ConfigPropertyLoader.getProperties();
             
    	
        String type = ConfigPropertyLoader.getProperty(ConfigPropertyNames.TRANSACTION_TYPE);
        	//connstrategy.getEnvironment().getProperty(ConfigPropertyNames.TRANSACTION_TYPE, ConfigPropertyNames.TRANSACTION_TYPES.LOCAL_TRANSACTION);
        if (type == null) {
        	type = ConfigPropertyNames.TRANSACTION_TYPES.LOCAL_TRANSACTION;
//        	throw new RuntimeException("Property " + ConfigPropertyNames.TRANSACTION_TYPE + " was specified");
        }
        
        System.out.println("Create TransactionContainer: " + type);
        
        if (type.equalsIgnoreCase(ConfigPropertyNames.TRANSACTION_TYPES.LOCAL_TRANSACTION)) {
        	transacton = new LocalTransaction();
        }
        else if (type.equalsIgnoreCase(ConfigPropertyNames.TRANSACTION_TYPES.XATRANSACTION)) {
        	transacton = new XATransaction();
        }
        else if (type.equalsIgnoreCase(ConfigPropertyNames.TRANSACTION_TYPES.JNDI_TRANSACTION)) {
        	transacton = new JNDITransaction();

        } else {
        	throw new TransactionRuntimeException("Invalid property value of " + type + " for " + ConfigPropertyNames.TRANSACTION_TYPE );
        }

        return transacton;
    }
    
}
