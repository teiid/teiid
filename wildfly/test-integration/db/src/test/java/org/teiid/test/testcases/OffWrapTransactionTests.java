/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.testcases;

import java.util.ArrayList;
import org.junit.Test;

import org.teiid.jdbc.AbstractQueryTest;
import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.ConfigPropertyNames.TXN_AUTO_WRAP_OPTIONS;
import org.teiid.test.framework.query.AbstractQueryTransactionTest;
import org.teiid.test.framework.query.QueryExecution;
import org.teiid.test.framework.transaction.TxnAutoTransaction;

@SuppressWarnings("nls")
public class OffWrapTransactionTests extends BaseAbstractTransactionTestCase {
    


    @Override
    protected TransactionContainer getTransactionContainter() {
	 return new TxnAutoTransaction(TXN_AUTO_WRAP_OPTIONS.AUTO_WRAP_OFF);
    }
    
    

    /**
     * Sources = 1
     * Commands = 1, Select
     * Batching = Full Processing, Single Connector Batch
     * result = commit 
     */
    @Test
    public void testSingleSourceSelect() throws Exception {

	AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testSingleSourceSelect") {
            public void testCase() throws Exception {
                execute("select * from pm1.g1 where pm1.g1.e1 < 100");
                assertRowCount(100);
            }
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);
    }



    /**
     * Sources = 1
     * Commands = 1, Update
     * Batching = Full Processing, Single Connector Batch
     * result = commit 
     */
    @Test
    public void testSingleSourceUpdate() throws Exception {
	AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testSingleSourceUpdate") {
            public void testCase() throws Exception {
                execute("insert into pm1.g1 (e1, e2) values(100, '100')");
            }
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);       

        // now verify the results
        AbstractQueryTest test = new QueryExecution(userTxn.getSource("pm1"));
        test.execute("select * from g1 where e1 = 100");
        test.assertRowCount(1);
    }
    
    /**
     * Sources = 1
     * Commands = multiple - Success
     * Batching = Full Processing, Single Connector Batch
     * result = commit
     */
    @Test
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
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);      

        // now verify the results
        AbstractQueryTest test = new QueryExecution(userTxn.getSource("pm1"));
        test.execute("select * from g1 where e1 >= 100");
        test.assertRowCount(10);
        test.execute("select * from g2 where e1 >= 100");
        test.assertRowCount(10);        
      
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

                for (int i = 200; i < 210; i++) {
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
        test.execute("select * from g1 where e1 >= 200");
        test.assertRowCount(10);
     
    }
    
    ///////////////////////////////////////////////////////////////////////////////////////////////
    //  Multiple Sources     - Rows from 500
    ///////////////////////////////////////////////////////////////////////////////////////////////
    
    /**
     * Sources = 2
     * Commands = 1, Select
     * Batching = Full Processing, Single Connector Batch
     * result = commit 
     */
    @Test
    public void testMultipleSourceSelect() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceSelect") {
            public void testCase() throws Exception {
                execute("select * from pm1.g1 join pm2.g1 on pm1.g1.e1 = pm2.g1.e1 where pm1.g1.e1 < 100");
                assertRowCount(100);
            }
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);
    }
    
    /**
     * Sources = 2
     * Commands = 1, Update
     * Batching = Full Processing, Single Connector Batch
     * result = commit 
     */
    @Test
    public void testMultipleSourceUpdate() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceUpdate") {
            public void testCase() throws Exception {
                execute("insert into vm.g1 (pm1e1, pm1e2, pm2e1, pm2e2) values(500, '500', 500, '500')");
            }
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);       
        
        // now verify the results
        AbstractQueryTest test = new QueryExecution(userTxn.getSource("pm1"));
        test.execute("select * from g1 where e1 = 500");
        test.assertRowCount(1);
        
        test = new QueryExecution(userTxn.getSource("pm2"));
        test.execute("select * from g1 where e1 = 500");
        test.assertRowCount(1);
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

                for (int i = 700; i < 710; i++) {
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
        test.execute("select * from g1 where e1 >= 700 and e1 < 710");
        test.assertRowCount(10);
      
        
        test = new QueryExecution(userTxn.getSource("pm2"));
        test.execute("select * from g1 where e1 >= 700 and e1 < 710");
        test.assertRowCount(10);        
      
    }     
    
    /**
     * Sources = 2
     * Commands = 1, Update
     * Batching = Full Processing, Single Connector Batch
     * result = commit 
     */
    @Test
    public void testMultipleSourceBulkRowInsert() throws Exception {

	
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceBulkRowInsert") {
            ArrayList list = new ArrayList();
            public void testCase() throws Exception {
                for (int i = 800; i < 807; i++) {
                    list.add("insert into pm1.g1 (e1, e2) values("+i+",'"+i+"')");
                    list.add("insert into pm2.g1 (e1, e2) values("+i+",'"+i+"')");
                }
                list.add("select pm1.g1.e1, pm1.g1.e2 into pm2.g2 from pm1.g1 where pm1.g1.e1 >= 800");
                
                // force the rollback by trying to insert an invalid row.
                list.add("insert into pm1.g2 (e1, e2) values(9999,'9999')");             

 //               execute((String[])list.toArray(new String[list.size()]));
                executeBatch((String[])list.toArray(new String[list.size()]), -1);
            }
            
            public boolean exceptionExpected() {
                return true;
            }            
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);       
        
        // now verify the results
        AbstractQueryTest test = new QueryExecution(userTxn.getSource("pm1"));
        test.execute("select * from g1 where e1 >= 800 and e1 < 807");
        test.assertRowCount(7);
        
        test = new QueryExecution(userTxn.getSource("pm2"));
        test.execute("select * from g1 where e1 >= 800 and e1 < 807");
        test.assertRowCount(7);
        test.execute("select * from g2 where e1 >= 800 and e1 < 807");
        test.assertRowCount(7);        

    }     
}
