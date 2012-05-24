/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.testcases;

import org.junit.Test;
import org.teiid.jdbc.AbstractQueryTest;
import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.query.AbstractQueryTransactionTest;
import org.teiid.test.framework.query.QueryExecution;
import org.teiid.test.framework.transaction.LocalTransaction;




/** 
 * User Transaction Test is where user handles all the transaction boundaries
 * so, autocmmit = OFF, and No transaction auto wrapping.
 */
@SuppressWarnings("nls")
public class LocalTransactionTests extends CommonTransactionTests {


    @Override
    protected TransactionContainer getTransactionContainter() {
	// TODO Auto-generated method stub
	return new LocalTransaction();
    }
    
    /**
     * Sources = 1
     * Commands = multiple - Success
     * Batching = Full Processing, Single Connector Batch
     * result = rollback
     */
    @Test
    public void testSingleSourceMultipleCommandsExplicitRollback() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testSingleSourceMultipleCommandsExplicitRollback") {
            @Override
	    public void testCase() throws Exception {
                for (int i = 200; i < 220; i++) {
                    execute("insert into pm1.g1 (e1, e2) values("+i+",'"+i+"')");
                    execute("insert into pm1.g2 (e1, e2) values("+i+",'"+i+"')");
                }                
            }
            
            @Override
	    public boolean rollbackAllways() {
                return true;
            }
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);      

        // now verify the results
        AbstractQueryTest test = new QueryExecution(userTxn.getSource("pm1"));
        test.execute("select * from g1 where e1 >= 200 and e1 < 220");
        test.assertRowCount(0);
        test.execute("select * from g2 where e1 >= 200 and e1 < 220");
        test.assertRowCount(0);        
      
    } 
    
    /**
     * Sources = 1
     * Commands = multiple - Success
     * Batching = Full Processing, Single Connector Batch
     * result = rollback
     */
    @Test
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
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);      

        // now verify the results
        AbstractQueryTest test = new QueryExecution(userTxn.getSource("pm1"));
        test.execute("select * from g1 where e1 >= 200 and e1 < 220");
        test.assertRowCount(0);
    
    }    
    
    /**
     * Sources = 2
     * Commands = multiple - Success
     * Batching = Full Processing, Single Connector Batch
     * result = rollback
     */
    @Test
    public void testMultipleSourceMultipleCommandsExplicitRollback() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceMultipleCommandsExplicitRollback") {
            public void testCase() throws Exception {

                for (int i = 700; i < 720; i++) {
                    Integer val = new Integer(i);
                    execute("insert into pm1.g1 (e1, e2) values(?,?)", new Object[] {val, val.toString()});
                    execute("insert into pm1.g2 (e1, e2) values(?,?)", new Object[] {val, val.toString()});
                    
                    execute("insert into pm2.g1 (e1, e2) values(?,?)", new Object[] {val, val.toString()});
                    execute("insert into pm2.g2 (e1, e2) values(?,?)", new Object[] {val, val.toString()});                    
                }                
            }
            
            // force the rollback
            public boolean rollbackAllways() {
                return true;
            }
            
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);      

        // now verify the results
        AbstractQueryTest test = new QueryExecution(userTxn.getSource("pm1"));
        test.execute("select * from g1 where e1 >= 700 and e1 < 720");
        test.assertRowCount(0);        
        
        test = new QueryExecution(userTxn.getSource("pm2"));
        test.execute("select * from g1 where e1 >= 700 and e1 < 720");
        test.assertRowCount(0);        
     
    }
    
    /**
     * Sources = 2
     * Commands = multiple - Success
     * Batching = Full Processing, Single Connector Batch
     * result = rollback
     */
    @Test
    public void testMultipleSourceMultipleCommandsReferentialIntegrityRollback() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceMultipleCommandsReferentialIntegrityRollback") {
            public void testCase() throws Exception {

                for (int i = 700; i < 720; i++) {
                    Integer val = new Integer(i);
                    execute("insert into pm1.g1 (e1, e2) values(?,?)", new Object[] {val, val.toString()});
                    execute("insert into pm1.g2 (e1, e2) values(?,?)", new Object[] {val, val.toString()});
                    
                    execute("insert into pm2.g1 (e1, e2) values(?,?)", new Object[] {val, val.toString()});
                    execute("insert into pm2.g2 (e1, e2) values(?,?)", new Object[] {val, val.toString()});                    
                }
                
                // force the rollback by trying to insert an invalid row.
                execute("insert into pm1.g2 (e1, e2) values(?,?)", new Object[] {new Integer(9999), "9999"});
            }
            
            public boolean exceptionExpected() {
                return true;
            }
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);      

        // now verify the results
        AbstractQueryTest test = new QueryExecution(userTxn.getSource("pm1"));
        test.execute("select * from g1 where e1 >= 700 and e1 < 720");
        test.assertRowCount(0);
  
        
        test = new QueryExecution(userTxn.getSource("pm2"));
        test.execute("select * from g1 where e1 >= 700 and e1 < 720");
        test.assertRowCount(0);        
     
    }
    
    /**
     * Sources = 2
     * Commands = 1, Update
     * Batching = Full Processing, Single Connector Batch
     * result = commit 
     */
    @Test
    public void testMultipleSourceBulkRowInsertRollback() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceBulkRowInsertRollback") {
            public void testCase() throws Exception {
                for (int i = 100; i < 120; i++) {
                    Integer val = new Integer(i);
                    execute("insert into vm.g1 (pm1e1, pm1e2, pm2e1, pm2e2) values(?,?,?,?)", new Object[] {val, val.toString(), val, val.toString()});
                }
                execute("select pm1.g1.e1, pm1.g1.e2 into pm2.g2 from pm1.g1 where pm1.g1.e1 >= 100");
                
                // force the rollback by trying to insert an invalid row.
                execute("insert into pm1.g2 (e1, e2) values(?,?)", new Object[] {new Integer(9999), "9999"});                
            }
            
            public boolean exceptionExpected() {
                return true;
            }
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);       
        
        // now verify the results
        AbstractQueryTest test = new QueryExecution(userTxn.getSource("pm1"));
        test.execute("select * from g1 where e1 >= 100 and e1 < 120");
        test.assertRowCount(0);
        
        test = new QueryExecution(userTxn.getSource("pm2"));
        test.execute("select * from g1 where e1 >= 100 and e1 < 120");
        test.assertRowCount(0);
        test.execute("select * from g2 where e1 >= 100 and e1 < 120");
        test.assertRowCount(0);        
    } 
    
    /**
     * Sources = 2
     * Commands = multiple - Success
     * Batching = Full Processing, Single Connector Batch
     * result = commit
     */
    @Test
    public void testMultipleSourceMultipleVirtualCommandsRollback() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceMultipleVirtualCommandsRollback") {
            public void testCase() throws Exception {

                for (int i = 600; i < 615; i++) {
                    Integer val = new Integer(i);
                    execute("insert into vm.g1 (pm1e1, pm1e2, pm2e1, pm2e2) values(?,?,?,?)", new Object[] {val, val.toString(), val, val.toString()});
                    execute("insert into vm.g2 (pm1e1, pm1e2, pm2e1, pm2e2) values(?,?,?,?)", new Object[] {val, val.toString(), val, val.toString()});                    
                }
                
                execute("update vm.g1 set pm1e2='blah' where pm1e1 >= 605");
                
                execute("delete from vm.g2 where vm.g2.pm1e1 >= 610");
                execute("delete from vm.g1 where vm.g1.pm1e1 >= 610");
                
                execute("select * from vm.g1 where pm1e1 >= 600 and pm1e1 < 615");
                assertRowCount(10);
                
                // force the rollback by trying to insert an invalid row.
                execute("insert into pm1.g2 (e1, e2) values(?,?)", new Object[] {new Integer(9999), "9999"});                                
            }
            
            public boolean exceptionExpected() {
                return true;
            }
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);      

        // now verify the results
        AbstractQueryTest test = new QueryExecution(userTxn.getSource("pm1"));
        test.execute("select * from g1 where e1 >= 600 and e1 < 615");
        test.assertRowCount(0);
        test.execute("select * from g2 where e1 >= 600 and e1 < 615");
        test.assertRowCount(0);
        test.execute("select distinct e2 from g1 where e1 >= 600 and e1 < 615");
        test.assertRowCount(0);
      
    }        
}
