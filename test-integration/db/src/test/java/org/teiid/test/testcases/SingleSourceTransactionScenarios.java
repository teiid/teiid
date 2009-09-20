/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.testcases;

import org.teiid.test.framework.AbstractQueryTransactionTest;
import org.teiid.test.framework.QueryExecution;
import org.teiid.test.framework.TransactionQueryTest;

import com.metamatrix.jdbc.api.AbstractQueryTest;



/** 
 * A common SingleSource test case among many different transaction stuff. 
 */
public class SingleSourceTransactionScenarios extends BaseAbstractTransactionTestCase {
    
    public SingleSourceTransactionScenarios(String name) {
        super(name);
    }
    
 
    ///////////////////////////////////////////////////////////////////////////////////////////////
    //  Single Source - Rows below 500 (for insert/update/delete)
    ///////////////////////////////////////////////////////////////////////////////////////////////
    
    /**
     * Sources = 1
     * Commands = 1, Select
     * Batching = Full Processing, Single Connector Batch
     * result = commit 
     */
    public void testSingleSourceSelect() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testSingleSourceSelect") {
            public void testCase() throws Exception {
                execute("select * from pm1.g1 where pm1.g1.e1 < 100");
                assertRowCount(100);
            }
            
        	public void validateTestCase() throws Exception {
        		
        	}
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);
        
        // there is nothing to verify here..
        
        System.out.println("Complete testSingleSourceSelect");

    }


	/**
     * Sources = 1
     * Commands = 1, Update
     * Batching = Full Processing, Single Connector Batch
     * result = commit 
     */
    public void testSingleSourceUpdate() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testSingleSourceUpdate") {
            public void testCase() throws Exception {
                execute("insert into pm1.g1 (e1, e2) values(100, '100')");
            }
        	public void validateTestCase() throws Exception {
                // now verify the results
                AbstractQueryTest test = new QueryExecution(getSource("pm1"));
                test.execute("select * from g1 where e1 = 100");
                test.assertRowCount(1);
                test.closeConnection();
       		
        	}
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);       
        
        
        System.out.println("Complete testSingleSourceUpdate");

    }
    

    /**
     * Sources = 1
     * Commands = 1, Update(prepared statement)
     * Batching = Full Processing, Single Connector Batch
     * result = commit 
     */
    public void testSingleSourcePreparedUpdate() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testSingleSourcePreparedUpdate") {
            public void testCase() throws Exception {
                execute("insert into pm1.g1 (e1, e2) values(?, ?)", new Object[] {new Integer(102), "102"});                
            }
           	public void validateTestCase() throws Exception {
                // now verify the results
                AbstractQueryTest test = new QueryExecution(getSource("pm1"));
                test.execute("select * from g1 where e1 = 102");
                test.assertRowCount(1);
                test.closeConnection();   
          	}
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);
        
         
        System.out.println("Complete testSingleSourcePreparedUpdate");

    }    
    
    /**
     * Sources = 1
     * Commands = multiple - Success
     * Batching = Full Processing, Single Connector Batch
     * result = commit
     */
    public void testSingleSourceMultipleCommands() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testSingleSourceMultipleCommands") {
            public void testCase() throws Exception {
                
                execute("delete from pm1.g1 where pm1.g1.e1 >= ?", new Object[] {new Integer(100)});
                
                execute("select * from pm1.g1");
                assertRowCount(100);
                
                for (int i = 100; i < 110; i++) {
                    Integer val = new Integer(i);
                    execute("insert into pm1.g1 (e1, e2) values(?,?)", new Object[] {val, val.toString()});
                    execute("insert into pm1.g2 (e1, e2) values(?,?)", new Object[] {val, val.toString()});
                }
            }
            
           	public void validateTestCase() throws Exception {
                // now verify the results
                AbstractQueryTest test = new QueryExecution(getSource("pm1"));
                test.execute("select * from g1 where e1 >= 100");
                test.assertRowCount(10);
                test.execute("select * from g2 where e1 >= 100");
                test.assertRowCount(10);        
                test.closeConnection();      
          	}
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);      

        
        System.out.println("Complete testSingleSourceMultipleCommands");

    }
        
    /**
     * Sources = 1
     * Commands = 1, Select
     * Batching = Partial Processing, Single Connector Batch
     * result = commit 
     */
    public void testSingleSourcePartialProcessing() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testSingleSourcePartialProcessing") {
            public void testCase() throws Exception {
                execute("select * from pm1.g1 where pm1.g1.e1 < 100 limit 10");
                assertRowCount(10);
            }
            
           	public void validateTestCase() throws Exception {
           	}
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);      
        
        System.out.println("Complete testSingleSourcePartialProcessing");

    }   
    
	   /**
     * Sources = 1
     * Commands = multiple - Success
     * Batching = Full Processing, Single Connector Batch
     * result = rollback
     */
    
    public void testSingleSourceMultipleCommandsExplicitRollback() throws Exception {
        // now it's empty
        AbstractQueryTest test = new QueryExecution(getSource("pm1"));
        		//getSource("pm1"));
        test.execute("select * from g1 where e1 >= 200 and e1 < 220");
        test.assertRowCount(0);
        test.execute("select * from g2 where e1 >= 200 and e1 < 220");
        test.assertRowCount(0);        
        test.closeConnection();  

    	
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testSingleSourceMultipleCommandsExplicitRollback") {
            public void testCase() throws Exception {
                for (int i = 200; i < 220; i++) {
                    execute("insert into pm1.g1 (e1, e2) values("+i+",'"+i+"')");
                    execute("insert into pm1.g2 (e1, e2) values("+i+",'"+i+"')");
                }                
            }
            
            public boolean rollbackAllways() {
                return true;
            }
            
           	public void validateTestCase() throws Exception {
                // now verify the results
           		AbstractQueryTest test = new QueryExecution(getSource("pm1"));
                		//getSource("pm1"));
                test.execute("select * from g1 where e1 >= 200 and e1 < 220");
                test.assertRowCount(0);
                test.execute("select * from g2 where e1 >= 200 and e1 < 220");
                test.assertRowCount(0);        
                test.closeConnection();  
          	}


        };        
        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);      
        
        System.out.println("Complete testSingleSourceMultipleCommandsExplicitRollback");

    } 

    /**
     * Sources = 1
     * Commands = multiple - Success
     * Batching = Full Processing, Single Connector Batch
     * result = rollback
     */
    public void testSingleSourceMultipleCommandsReferentialIntegrityRollback() throws Exception {
    	
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testSingleSourceMultipleCommandsReferentialIntegrityRollback") {
            public void testCase() throws Exception {
                for (int i = 200; i < 220; i++) {
                    Integer val = new Integer(i);
                    execute("insert into pm1.g1 (e1, e2) values(?,?)", new Object[] {val, val.toString()});
                    execute("insert into pm1.g2 (e1, e2) values(?,?)", new Object[] {val, val.toString()});
                }
                
                // force the rollback by trying to insert an invalid row.
                execute("insert into pm1.g2 (e1, e2) values(?,?)", new Object[] {new Integer(9999), "9999"});
            }
            
            public boolean exceptionExpected() {
                return true;
            }
            
           	public void validateTestCase() throws Exception {
                // now verify the results
                AbstractQueryTest test = new QueryExecution(getSource("pm1"));
                test.execute("select * from g1 where e1 >= 200 and e1 < 220");
                test.assertRowCount(0);
                test.closeConnection();   
          	}
        };        
        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);      
        
        System.out.println("Complete testSingleSourceMultipleCommandsReferentialIntegrityRollback");

    }    

  
    
 
}
