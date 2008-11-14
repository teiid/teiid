/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.platform.config.spi.xml;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import com.metamatrix.platform.config.BaseTest;
import com.metamatrix.platform.config.transaction.ConfigTransactionLock;
import com.metamatrix.platform.config.transaction.ConfigTransactionLockFactory;
import com.metamatrix.platform.config.transaction.ConfigUserTransaction;
import com.metamatrix.platform.config.transaction.ConfigUserTransactionFactory;

public class JDBCConfigTransactionLocking extends BaseTest {
	
	private static String PRINCIPAL = "JDBCConfigTransactionLocking";   //$NON-NLS-1$
	private static final String JDBC_CONFIG_PROPERTIES = "configjdbc.properties";     //$NON-NLS-1$
    
    private static Properties props = null;

    public JDBCConfigTransactionLocking(String name) {
        super(name, true);
        
        this.printMessages = true;
    }

	protected void setUp() throws Exception {
		super.setUp();
			getProperties();
			     	    
		
	}        
    
    
    
    protected Properties getProperties() throws Exception {
    	if (props != null) {
    		return props;
    	}
    	
    	File f = new File(getPath(), JDBC_CONFIG_PROPERTIES);
    	if (!f.exists()) {
    		fail(JDBC_CONFIG_PROPERTIES + " does not exist"); //$NON-NLS-1$
    	}
    	
    	props = new Properties();
    	
    	
    	FileInputStream fis = new FileInputStream(f);
    	

    	props.load(fis);
//    	PropertyLoader pl = new PropertyLoader(f.getPath());
    	
    	    		// these system props need to be set for the CurrentConfiguration call
// set the properties for the resource pool info needed by IDGenerator
     		Properties sysProps = System.getProperties();
     		sysProps.putAll(props);
     		System.setProperties(sysProps);
     		
    	printMsg("Initialized Properties and Crypto"); //$NON-NLS-1$
        
    	
    	return props;
    }
  
    /**
    * Basic test, does it work
    * Test the following:
    * <li> can a read transaction be obtained
    */
    public void testLocking() throws Exception {
    	printMsg("Starting testLocking"); //$NON-NLS-1$
    	
		XMLConfigurationTransactionFactory transFactory = new XMLConfigurationTransactionFactory(getProperties());
      	
      	
		ConfigTransactionLockFactory lockFactory = transFactory.getTransactionLockFactory();
		
		
		ConfigTransactionLock lock = lockFactory.obtainConfigTransactionLock(PRINCIPAL, ConfigTransactionLock.LOCK_CONFIG_CHANGING, false);
		
		printMsg("Obtained 1st Lock"); //$NON-NLS-1$
		if (lock == null) {
			fail("Unable to obtain lock."); //$NON-NLS-1$
		}
		
		if (lockFactory.getCurrentConfigTransactionLock() == null) {
			fail("Lock was not save in the pesistent layer."); //$NON-NLS-1$
		}
		
		printMsg("Obtained lock " + lock + " now release it"); //$NON-NLS-1$ //$NON-NLS-2$
			
		lockFactory.releaseConfigTransactionLock(lock);
		
		ConfigTransactionLock olock = lockFactory.getCurrentConfigTransactionLock();
		if (olock != null) {
			fail("Lock " + olock.toString() + " was not released."); //$NON-NLS-1$ //$NON-NLS-2$
		}
    	
    	printMsg("Completed testLocking"); //$NON-NLS-1$

    }
    
    
    public void xtestWriteTransaction() {
    	
    	printMsg("Starting testWriteTransaction"); //$NON-NLS-1$
    	
      ConfigUserTransaction userTrans = null;              
      try {
      	
			XMLConfigurationTransactionFactory transFactory = new XMLConfigurationTransactionFactory(getProperties());
      	
      	
	        ConfigUserTransactionFactory factory = new ConfigUserTransactionFactory(transFactory);
	        
	        
	        userTrans = factory.createWriteTransaction(PRINCIPAL);
	        
	        if (userTrans == null) {
	        	fail("Unable to obtain a user write transaction, userTrans is null"); //$NON-NLS-1$
	        }
	        
	        ConfigTransactionLock lock = transFactory.getTransactionLockFactory().getCurrentConfigTransactionLock();
	        if (!lock.equals(userTrans.getTransaction().getTransactionLock())) {
	        	fail("The user transaction lock held by " + userTrans.getTransaction().getLockAcquiredBy() +  //$NON-NLS-1$
	        		" is not the same as the current lock held by " + lock.getLockHolder()); //$NON-NLS-1$
	        }
	        
	        userTrans.begin();
	        userTrans.commit();
	        
	        
	        lock = transFactory.getTransactionLockFactory().getCurrentConfigTransactionLock();
	        if (lock != null) {
	        	fail("The commit of the transaction did not release the lock"); //$NON-NLS-1$
	        }
	         
	                        
        } catch (Exception e) {
        	System.out.println(e.getMessage());
        	fail(e.getMessage());
        
    	} 
    	
    	printMsg("Completed testWriteTransaction"); //$NON-NLS-1$

    }
        
    
    

}
