/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.common.connection;

import java.util.Properties;

import com.metamatrix.common.properties.UnmodifiableProperties;
import com.metamatrix.core.util.ReflectionHelper;

public class TransactionMgr {
    
    /**
     * The environment property name for the class that is to be used for the ManagedConnectionFactory implementation.
     * This property is required (there is no default).
     */
    public static final String FACTORY = "metamatrix.common.connection.transaction.factory"; //$NON-NLS-1$
    
    
    private TransactionFactory connectionFactory;
    
    private Properties properties;   
    
    private String userName;     
    
    public TransactionMgr( Properties properties, String userName ) throws ManagedConnectionException {
        this.userName = userName;
        if ( properties == null ) {
            this.properties = new Properties();
        } else {
            synchronized(properties) {
                this.properties = (Properties) properties.clone();
            }
        }
        if ( !(this.properties instanceof UnmodifiableProperties) ) {
            this.properties = new UnmodifiableProperties(this.properties);
        }

        // Create the proper factory instance
        String connectionFactoryClassName = this.properties.getProperty(FACTORY);
        try {
            this.connectionFactory = (TransactionFactory) ReflectionHelper.create(connectionFactoryClassName, null, getClass().getClassLoader());
        } catch(Exception e) {
            throw new ManagedConnectionException(e);
        }


    }
    
    public TransactionInterface getReadTransaction() throws ManagedConnectionException {
        // Get a connection for the read (this may fail) ...
        
          ManagedConnection connection = this.connectionFactory.createConnection(this.properties, userName);
          connection.open();
         connection.setForRead();

       
        // Upon success, create transaction instance ...
        return this.connectionFactory.createTransaction(connection,true);
    }

    public TransactionInterface getWriteTransaction() throws ManagedConnectionException {
        // Get a connection for the read (this may fail) ...
          ManagedConnection connection = this.connectionFactory.createConnection(this.properties, userName);
          connection.open();
          connection.setForWrite();

        // Upon success, create transaction instance ...
        return this.connectionFactory.createTransaction(connection,false);
    }    


}
