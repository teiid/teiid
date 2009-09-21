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

    	
    	ConnectionStrategy connstrategy = ConnectionStrategyFactory.getInstance().getConnectionStrategy();
             
    	
        String type = connstrategy.getEnvironment().getProperty(ConfigPropertyNames.TRANSACTION_TYPE, ConfigPropertyNames.TRANSACTION_TYPES.LOCAL_TRANSACTION);
        if (type == null) {
        	throw new RuntimeException("Property " + ConfigPropertyNames.TRANSACTION_TYPE + " was specified");
        }
        
        if (type.equalsIgnoreCase(ConfigPropertyNames.TRANSACTION_TYPES.LOCAL_TRANSACTION)) {
        	transacton = new LocalTransaction(connstrategy);
        }
        else if (type.equalsIgnoreCase(ConfigPropertyNames.TRANSACTION_TYPES.XATRANSACTION)) {
        	transacton = new XATransaction(connstrategy);
        }
        else if (type.equalsIgnoreCase(ConfigPropertyNames.TRANSACTION_TYPES.JNDI_TRANSACTION)) {
        	transacton = new JNDITransaction(connstrategy);

        } else {
        	throw new TransactionRuntimeException("Invalid property value of " + type + " for " + ConfigPropertyNames.TRANSACTION_TYPE );
        }

        return transacton;
    }
    
}
