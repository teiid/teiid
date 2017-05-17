/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.test.client;

import org.teiid.test.framework.ConfigPropertyLoader;
import org.teiid.test.framework.TestLogger;
import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.ConfigPropertyNames.TXN_AUTO_WRAP_OPTIONS;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;
import org.teiid.test.framework.transaction.JNDITransaction;
import org.teiid.test.framework.transaction.LocalTransaction;
import org.teiid.test.framework.transaction.TxnAutoTransaction;
import org.teiid.test.framework.transaction.XATransaction;


/**
 * TransactionFactory is used so that the type of {@link TransactionContainer } can be dynamically loaded
 * based on a property.
 * 
 * Specify the property {@link #TRANSACTION_TYPE} in order to set the transaction type to use.
 * 
 * @author vanhalbert
 *
 */
@SuppressWarnings("nls")
public class TransactionFactory {
    

	/**
	 * Transaction Type indicates the type of transaction container to use
	 * @see TransactionFactory
	 */
    public static final String TRANSACTION_TYPE = "transaction-option"; //$NON-NLS-1$

    public interface TRANSACTION_TYPES {
		public static final String LOCAL_TRANSACTION = "local";     //$NON-NLS-1$
		public static final String XATRANSACTION = "xa"; //$NON-NLS-1$
		public static final String JNDI_TRANSACTION = "jndi"; //$NON-NLS-1$
		public static final String OFFWRAP_TRANSACTION = "off"; //$NON-NLS-1$
		public static final String ONWRAP_TRANSACTION = "on"; //$NON-NLS-1$
		public static final String AUTOWRAP_TRANSACTION = "auto"; //$NON-NLS-1$
   }
	

        
    private TransactionFactory(){}
        
    
    public static TransactionContainer create(ConfigPropertyLoader config) throws QueryTestFailedException {
    	TransactionContainer transacton = null;
    	    	
        String type = config.getProperty(TRANSACTION_TYPE);
        if (type == null) {
            throw new TransactionRuntimeException(TRANSACTION_TYPE + " property was not specified" );
        } 
        
       
        TestLogger.logDebug("====  Create Transaction-Option: " + type);
        
        if (type.equalsIgnoreCase(TRANSACTION_TYPES.LOCAL_TRANSACTION)) {
        	transacton = new LocalTransaction();
        }
        else if (type.equalsIgnoreCase(TRANSACTION_TYPES.XATRANSACTION)) {
        	transacton = new XATransaction();
        }
        else if (type.equalsIgnoreCase(TRANSACTION_TYPES.JNDI_TRANSACTION)) {
        	transacton = new JNDITransaction();
        }
     	else if (type.equalsIgnoreCase(TRANSACTION_TYPES.OFFWRAP_TRANSACTION)) {
            	transacton = new TxnAutoTransaction(TXN_AUTO_WRAP_OPTIONS.AUTO_WRAP_OFF);
        }
        else if (type.equalsIgnoreCase(TRANSACTION_TYPES.ONWRAP_TRANSACTION)) {
        	transacton = new TxnAutoTransaction(TXN_AUTO_WRAP_OPTIONS.AUTO_WRAP_ON);
        }
            else if (type.equalsIgnoreCase(TRANSACTION_TYPES.AUTOWRAP_TRANSACTION)) {
        	transacton = new TxnAutoTransaction(TXN_AUTO_WRAP_OPTIONS.AUTO_WRAP_AUTO);

        } else {
        	throw new TransactionRuntimeException("Invalid property value of " + type + " for " + TRANSACTION_TYPE );
        }

        TestLogger.log("====  TransactionContainer: " + transacton.getClass().getName() + " option:" + type);
        return transacton;
    }
    
}
