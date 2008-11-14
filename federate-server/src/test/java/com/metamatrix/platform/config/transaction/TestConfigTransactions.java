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

package com.metamatrix.platform.config.transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import com.metamatrix.common.transaction.TransactionException;
import com.metamatrix.common.transaction.TransactionNotSupportedException;
import com.metamatrix.common.transaction.UserTransaction;
import com.metamatrix.platform.config.BaseTest;

public class TestConfigTransactions extends BaseTest {
	

    public TestConfigTransactions(String name) {
        super(name);   
        
        printMessages = true;     
    }

    /**
    * Basic test, does it work
    * Test the following:
    * <li> can a read transaction be obtained
    */
    public void testReadTransaction() {
    	printMsg("Starting readTransaction"); //$NON-NLS-1$
    	
      UserTransaction userTrans = null;              
      try {
      	
		ConfigurationTransactionFactoryTest transFactory = new ConfigurationTransactionFactoryTest();
      	
      	
	        ConfigUserTransactionFactory factory = new ConfigUserTransactionFactory(transFactory);
	        
	        
	        userTrans = factory.createReadTransaction("TestConfigTransactions.testReadTransaction"); //$NON-NLS-1$
	        
	        if (userTrans == null) {
	        	fail("Unable to obtain a user read transaction, userTrans is null"); //$NON-NLS-1$
	        }
	        
	        userTrans.begin();
	        userTrans.commit();
	                        
        } catch (Exception e) {
        	System.out.println(e.getMessage());
        	fail(e.getMessage());
        
    	} 
    	
    	printMsg("Completed readTransaction"); //$NON-NLS-1$

    }
    
    
    /**
    * Basic test, does it work
    * Test the following:
    * <li> can a read transaction be obtained
    */
    public void testWriteTransaction() {
    	printMsg("Starting testWriteTransaction"); //$NON-NLS-1$
    	
      UserTransaction userTrans = null;              
      try {
      	
		ConfigurationTransactionFactoryTest transFactory = new ConfigurationTransactionFactoryTest();
      	
      	
	        ConfigUserTransactionFactory factory = new ConfigUserTransactionFactory(transFactory);
	        
	        userTrans = factory.createWriteTransaction("TestConfigTransactions.testReadTransaction"); //$NON-NLS-1$
	        
	        if (userTrans == null) {
	        	fail("Unable to obtain a user write transaction, userTrans is null"); //$NON-NLS-1$
	        }
	        
	        userTrans.begin();
	        userTrans.commit();
	                        
        } catch (Exception e) {
        	e.printStackTrace();
        	fail(e.getMessage());
        
    	} 
    	
    	printMsg("Completed testWriteTransaction"); //$NON-NLS-1$

    }
    
    
    /**
    * Test the following:
    * <li> can multiple write transactions be simultaneously obtained using the same thread
    * 
    * THIS TEST SHOULD FAIL - ONLY ONE TRANSACTION PER THREAD IS VALID AT A TIME
	*
    * NOTE: This test will not work because the same thread cannot
    * obtain multiple transactions.
    */
    public void testNegSimultaneousWriteSameThread() {
    	printMsg("Starting testNegSimultaneousReadTransactions"); //$NON-NLS-1$
     	
     	/**
     	 * This test will not work because the same thread cannot
     	 * obtain multiple transactions.
     	 */
      int num = 5;  
	  Collection trans = new ArrayList();                  
      try {
		ConfigurationTransactionFactoryTest transFactory = new ConfigurationTransactionFactoryTest();
      	
      	
	        ConfigUserTransactionFactory factory = new ConfigUserTransactionFactory(transFactory);
	      	
	      	for (int i = 0; i<num; i++) {
	      		String user = "TestConfigTransactions.testWriteTransaction" + i; //$NON-NLS-1$
	        	UserTransaction userTrans = factory.createWriteTransaction(user);
	       		if (userTrans == null) {
	        		fail("Unable to obtain a read transaction for try number " + i + ", userTrans is null"); //$NON-NLS-1$ //$NON-NLS-2$
	        	}
	        	userTrans.begin();	        	       	
				trans.add(userTrans);
	      	}	
	      	
	      	// this should not be reached because the exception should have been thrown
	      	for (Iterator it=trans.iterator(); it.hasNext(); ) {
	      		UserTransaction ut = (UserTransaction) it.next();
	      		ut.commit();
	      	}
      	                                 
        } catch (Exception e) {
//        	e.printStackTrace();
        	// The ConfigTransactionManager throws this exception because
        	// the same thread cannot obtain multiple transactons.
        	if (e instanceof TransactionException) {
        		printMsg("negSimultaneousWriteTransactions failed correctly"); //$NON-NLS-1$
        		
		      	// this should not be reached because the exception should have been thrown
		      	for (Iterator it=trans.iterator(); it.hasNext(); ) {
		      		UserTransaction ut = (UserTransaction) it.next();
		      		try {
			      		ut.rollback();
		      		} catch (Exception re) {
		      		}
		      	}
        			
        	} else {

	    		fail(e.getMessage());
        	}
    	}
    	
    	printMsg("Completed testNegSimultaneousReadTransactions"); //$NON-NLS-1$
    	

    } 
    
    
    /**
    * Basic test, does it work
    * Test the following:
    * <li> can multiple read transactions be simultaneously obtained
    * 
    * THIS TEST SHOULD FAIL - ONLY ONE TRANSACTION PER THREAD IS VALID AT A TIME
	*
    * NOTE: This test will not work because the same thread cannot
    * obtain multiple transactions.
    */
    public void testNegSimultaneousReadSameThread() {
    	printMsg("Starting testNegSimultaneousReadTransactions"); //$NON-NLS-1$
     	
     	/**
     	 * This test will not work because the same thread cannot
     	 * obtain multiple transactions.
     	 */
      int num = 5;              
      try {
		ConfigurationTransactionFactoryTest transFactory = new ConfigurationTransactionFactoryTest();
      	
      	
	        ConfigUserTransactionFactory factory = new ConfigUserTransactionFactory(transFactory);
	      	
	      	Collection trans = new ArrayList();
	      	for (int i = 0; i<num; i++) {
	      		String user = "TestConfigTransactions.testReadTransaction" + i; //$NON-NLS-1$
	        	UserTransaction userTrans = factory.createReadTransaction(user);
	       		if (userTrans == null) {
	        		fail("Unable to obtain a read transaction for try number " + i + ", userTrans is null"); //$NON-NLS-1$ //$NON-NLS-2$
	        	}
	        	userTrans.begin();	        	       	
				trans.add(userTrans);
	      	}	
	      	
	      	// this should not be reached because the exception should have been thrown
	      	for (Iterator it=trans.iterator(); it.hasNext(); ) {
	      		UserTransaction ut = (UserTransaction) it.next();
	      		ut.commit();
	      	}
      	                                 
        } catch (Exception e) {
        	// The ConfigTransactionManager throws this exception because
        	// the same thread cannot obtain multiple transactons.
        	if (e instanceof TransactionNotSupportedException) {
        		printMsg("negSimultaneousReadTransactions failed correctly"); //$NON-NLS-1$
        	} else {

	    		fail(e.getMessage());
        	}
    	}
    	
    	printMsg("Completed testNegSimultaneousReadTransactions"); //$NON-NLS-1$
    	

    } 
    
    /**
     * Test that mutliple threads can obtain a read transaction at the same time
     * Each thread can ONLY OBTAIN ON READ TRANSACTION, otherwise see 
     * NOTE on test {see #testNegSimultaneousReadTransactions}
     */
    public void testMultiThreadSimultaneousReadTransactions() {
    	printMsg("Starting testMultiThreadSimultaneousReadTransactions"); //$NON-NLS-1$
    	
		ConfigurationTransactionFactoryTest transFactory = new ConfigurationTransactionFactoryTest();
      	
      	
	        ConfigUserTransactionFactory factory = new ConfigUserTransactionFactory(transFactory);
    	
    	int count;
    	int threadCnt = 500; // number of threads to run
    	int threadTries = 1; // ONLY USE ONE,
    	
        ReadTransThread[] ts = new ReadTransThread[threadCnt];
    	
          for (count = 0; count < threadCnt; count++) {
              ts[count] = new ReadTransThread(factory, threadTries, count);
          }
          
        Exception e = helperMultiThreadSimultaneousTransactions(threadCnt, threadTries, ts);
        if (e != null) {
			fail(e.getMessage());
        } 
            	
    	printMsg("Completed testMultiThreadSimultaneousReadTransactions"); //$NON-NLS-1$
    	
    }
    
    /**
     * Test that mutliple threads can obtain a read transaction at the same time
     * 
     * THIS TEST SHOULD FAIL - ONLY ONE TRANSACTION PER THREAD IS VALID AT A TIME
     * 
     * This options changes the <code>threadTries</code> to 2 so that
     * obtaining a transaction for the same thread should fail.
     * 
     */
    public void testNegMultiThreadSimultaneousReadSameThread() {
    	printMsg("Starting testNegMultiThreadSimultaneousReadTransactions"); //$NON-NLS-1$
    	
		ConfigurationTransactionFactoryTest transFactory = new ConfigurationTransactionFactoryTest();
      	
      	
	        ConfigUserTransactionFactory factory = new ConfigUserTransactionFactory(transFactory);
    	
    	int count;
    	int threadCnt = 500; // number of threads to run
    	int threadTries = 2; // ONLY USE ONE,
    	
        ReadTransThread[] ts = new ReadTransThread[threadCnt];
    	
          for (count = 0; count < threadCnt; count++) {
              ts[count] = new ReadTransThread(factory, threadTries, count);
          }
          
        Exception e = helperMultiThreadSimultaneousTransactions(threadCnt, threadTries, ts);
        if (e == null) {
        	fail("testNegMultiThreadSimultaneousReadTransactions should have thrown an Exception " + //$NON-NLS-1$
        		"indicating that only one transaction per thread can be obtained."); //$NON-NLS-1$
        } else if (e instanceof TransactionException) {
        } else {
        	fail(e.getMessage());	
        }
    	
    	printMsg("Completed testNegMultiThreadSimultaneousReadTransactions"); //$NON-NLS-1$
    	
    }
    
    
    /**
     * Test that mutliple threads can obtain a write transaction at the same time
     * 
     * THIS TEST SHOULD NOT FAIL - The retry logic should allow all threads
     * to eventually obtain a lock.
     * 
     */
    public void testMultiThreadSimultaneousWriteTransactions() {
    	printMsg("Starting testMultiThreadSimultaneousWriteTransactions"); //$NON-NLS-1$
    	
		ConfigurationTransactionFactoryTest transFactory = new ConfigurationTransactionFactoryTest();
      	
      	
	        ConfigUserTransactionFactory factory = new ConfigUserTransactionFactory(transFactory);
    	
    	int count;
    	int threadCnt = 25; // number of threads to run
    	int threadTries = 1; // ONLY USE ONE,  
    	
    	  	
    	
        WriteTransThread[] ts = new WriteTransThread[threadCnt];
    	
          for (count = 0; count < threadCnt; count++) {
              ts[count] = new WriteTransThread(factory, threadTries, count);
          }
          
        Exception e = helperMultiThreadSimultaneousTransactions(threadCnt, threadTries, ts);
        if (e != null) {
        	e.printStackTrace();
        	
        	fail(e.getMessage());	

        }
    	
    	printMsg("Completed testMultiThreadSimultaneousWriteTransactions"); //$NON-NLS-1$
    	
    }
    
    
    
    private Exception helperMultiThreadSimultaneousTransactions(int threadCnt, 
    				int threadTries, 
    				BaseThread[] ts) {
    	

		// join the threads to finish together
            try {
                  for(int k = 0; k < threadCnt; k++){
                  	 ts[k].start();
                      ts[k].join();
                  }
            } catch (InterruptedException e) {
            }

//            int cntEs = 0;
            Exception te = null;
            for(int k = 0; k < threadCnt; k++){
              if (ts[k].hasException()) {
                  Exception e = ts[k].getException();
                  te = e;
                  break;

              }
            }
            
            return te;
                	
    } 
    
    
        
    protected class ReadTransThread extends BaseThread{
      private ConfigUserTransactionFactory factory;

      public ReadTransThread(ConfigUserTransactionFactory factory, int tries, int num) {
          super(tries, num);
          this.factory = factory;
      }
    	public void run(){
			Collection trans = new ArrayList();
	        for (int i=0; i < perThreadCnt; i++ ) {
	
	            try {
		      	
		        	UserTransaction userTrans = factory.createReadTransaction(objName);
		       		if (userTrans == null) {
		        		fail("Unable to obtain a read transaction in " + objName + "for try number " + i + ", userTrans is null"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		        	}
		        	
		        	
		        	userTrans.begin();
//		        	printMsg("<" + objName + ">Begin trans in thread ");
					trans.add(userTrans);
	
	
	            } catch (Exception toe) {
	                setException(toe);
	
	            }
	            // yield to the other thread 
	            yield();
	        }
	        try {
		      	for (Iterator it=trans.iterator(); it.hasNext(); ) {
		      		UserTransaction ut = (UserTransaction) it.next();
		      		ut.commit();
//		        	printMsg("<" + objName + ">Committed trans in thread ");
		      		
		      	}
	        } catch (Exception te) {
	                setException(te);
	
	       }
	        
       }	
    }
    
    protected class WriteTransThread extends BaseThread{
      private ConfigUserTransactionFactory factory;

      public WriteTransThread(ConfigUserTransactionFactory factory, int tries, int num) {
          super(tries, num);
          this.factory = factory;
      }
    	public void run(){
    		printMsg("<" + objName + ">Start thread " + new Date()); //$NON-NLS-1$ //$NON-NLS-2$
			Collection trans = new ArrayList();
	        for (int i=0; i < perThreadCnt; i++ ) {
	
	            try {
		      	
		        	UserTransaction userTrans = ConfigTransactionHelper.getWriteTransactionWithRetry(objName, factory);
		       		if (userTrans == null) {
		        		fail("Unable to obtain a write transaction in " + objName + "for try number " + i + ", userTrans is null"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		        	}
		        	
//		        	printMsg("<" + objName + ">Begin write trans in thread ");
//		        	userTrans.begin();
		        	printMsg("<" + objName + ">Started write trans in thread "); //$NON-NLS-1$ //$NON-NLS-2$
					trans.add(userTrans);
	
	
	            } catch (Exception toe) {
	                setException(toe);
	
	            }
	            // yield to the other thread, and so to hold onto the lock for a short period
	            yield();
	        }
	        try {
		      	for (Iterator it=trans.iterator(); it.hasNext(); ) {
		      		UserTransaction ut = (UserTransaction) it.next();
		      		ut.commit();
		        	printMsg("<" + objName + ">Committed write trans in thread "); //$NON-NLS-1$ //$NON-NLS-2$
		      		
		      	}
	        } catch (Exception te) {
	                setException(te);
	
	       }
	        
       }	
       
       
    }
    

    protected class BaseThread extends Thread{
    	protected String objName ;
      protected int perThreadCnt = 1;
      private Exception t = null;


      public BaseThread(int iterationCnt, int num) {
          perThreadCnt = iterationCnt;
          objName = "Thread " + num; //$NON-NLS-1$
      }

      public Exception getException() {
          return t;
      }

      public void setException(Exception te) {
          t = te;
      }

      public boolean hasException() {
          return (t==null ? false : true);
      }

    }
    

}
