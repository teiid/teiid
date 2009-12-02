/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.testcases;

import java.util.ArrayList;

import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.query.AbstractQueryTransactionTest;
import org.teiid.test.framework.query.QueryExecution;
import org.teiid.test.framework.transaction.OptimisticWrapTransaction;

import com.metamatrix.jdbc.api.AbstractQueryTest;



/** 
 * The main thing to test in this is, when the single source should is involved it should work
 * fine, when multiple sources involved it should fail.
 */
public class OptimisticWrapTransactionTests extends BaseAbstractTransactionTestCase {

    public OptimisticWrapTransactionTests(String testName) {
        super(testName);
    }
    
    protected TransactionContainer getTransactionContainter() {
        return new OptimisticWrapTransaction();
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
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest() {
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
     * Commands = 1, Update(prepared statement)
     * Batching = Full Processing, Single Connector Batch
     * result = commit 
     */
    public void testSingleSourcePreparedUpdate() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest() {
            public void testCase() throws Exception {
                execute("insert into pm1.g1 (e1, e2) values(?, ?)", new Object[] {new Integer(102), "102"});
            }
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);
        
        // now verify the results
        AbstractQueryTest test = new QueryExecution(userTxn.getSource("pm1"));
        test.execute("select * from g1 where e1 = 102");
        test.assertRowCount(1);
        test.closeConnection();        
    }    
    
    /**
     * Sources = 1
     * Commands = multiple - Success
     * Batching = Full Processing, Single Connector Batch
     * result = commit
     */
    public void testSingleSourceMultipleCommands() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest() {
            public void testCase() throws Exception {
                execute("delete from pm1.g2 where pm1.g2.e1 >= ?", new Object[] {new Integer(100)});
                execute("delete from pm1.g1 where pm1.g1.e1 >= ?", new Object[] {new Integer(100)});
                
                execute("select * from pm1.g1");
                assertRowCount(100);
                for (int i = 100; i < 120; i++) {
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
        test.execute("select * from g1 where e1 >= 100 and e1 < 120");
        test.assertRowCount(20);
        test.execute("select * from g2 where e1 >= 100 and e1 < 120");
        test.assertRowCount(20);        
        test.closeConnection();        
    }    
    
    /**
     * Sources = 1
     * Commands = multiple - Success
     * Batching = Full Processing, Single Connector Batch
     * result = commit
     */
    public void xtestSingleSourceBatchCommand() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest() {
            public void testCase() throws Exception {
                ArrayList list = new ArrayList();
                list.add("delete from pm1.g2 where pm1.g2.e1 >= 100");
                list.add("delete from pm1.g1 where pm1.g1.e1 >= 100");

                for (int i = 200; i < 205; i++) {
                    list.add("insert into pm1.g1 (e1, e2) values("+i+",'"+i+"')");
                    list.add("insert into pm1.g2 (e1, e2) values("+i+",'"+i+"')");
                }
                executeBatch((String[])list.toArray(new String[list.size()]));
            }
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);      

        // now verify the results
        AbstractQueryTest test = new QueryExecution(userTxn.getSource("pm1"));
        test.execute("select * from g1 where e1 >= 200 and e1 < 205");
        test.assertRowCount(5);
        test.execute("select * from g2 where e1 >= 200 and e1 < 205");
        test.assertRowCount(5);        
        test.closeConnection();        
    }     
    
    /**
     * Sources = 1
     * Commands = 1, Update(prepared statement)
     * Batching = Full Processing, Single Connector Batch
     * result = commit 
     */
    public void testSingleSourcePreparedUpdateRollback() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest() {
            public void testCase() throws Exception {
                execute("insert into pm1.g2 (e1, e2) values(?, ?)", new Object[] {new Integer(9999), "9999"});
            }
            
            public boolean exceptionExpected() {
                return true;
            }
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);
        
        // now verify the results
        AbstractQueryTest test = new QueryExecution(userTxn.getSource("pm1"));
        test.execute("select * from g2 where e1 = 9999");
        test.assertRowCount(0);
        test.closeConnection();        
    }     
    
    
    /**
     * Sources = 1
     * Commands = multiple - Success
     * Batching = Full Processing, Single Connector Batch
     * result = commit
     */
    public void testSingleSourceMultipleCommandsRollback() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest() {
            public void testCase() throws Exception {
                execute("delete from pm1.g2 where pm1.g2.e1 >= ?", new Object[] {new Integer(100)});
                execute("delete from pm1.g1 where pm1.g1.e1 >= ?", new Object[] {new Integer(100)});
                
                execute("select * from pm1.g1");
                assertRowCount(100);
                
                for (int i = 300; i < 310; i++) {
                    Integer val = new Integer(i);
                    execute("insert into pm1.g1 (e1, e2) values(?,?)", new Object[] {val, val.toString()});
                    execute("insert into pm1.g2 (e1, e2) values(?,?)", new Object[] {val, val.toString()});
                }
                
                // this will make it rollback
                execute("insert into pm1.g2 (e1, e2) values(?, ?)", new Object[] {new Integer(9999), "9999"});                
            }
            
            public boolean exceptionExpected() {
                return true;
            }

        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);      

        // now verify the results (since they are not bundled in single command they should be treated individually)
        AbstractQueryTest test = new QueryExecution(userTxn.getSource("pm1"));
        test.execute("select * from g1 where e1 >= 300 and e1 < 310");
        test.assertRowCount(10);
        test.execute("select * from g2 where e1 >= 300 and e1 < 310");
        test.assertRowCount(10);      
        test.execute("select * from g2 where e1 = 9999");
        test.assertRowCount(0);        
        test.closeConnection();        
    }     
    
    /**
     * Sources = 1
     * Commands = multiple - Success
     * Batching = Full Processing, Single Connector Batch
     * result = commit
     */
    public void testSingleSourceBatchCommandRollback() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest() {
            public void testCase() throws Exception {
                ArrayList list = new ArrayList();
                list.add("delete from pm1.g2 where pm1.g2.e1 >= 100");
                list.add("delete from pm1.g1 where pm1.g1.e1 >= 100");

                for (int i = 400; i < 410; i++) {
                    list.add("insert into pm1.g1 (e1, e2) values("+i+",'"+i+"')");
                    list.add("insert into pm1.g2 (e1, e2) values("+i+",'"+i+"')");
                }

                // this will make it rollback
                list.add("insert into pm1.g2 (e1, e2) values(9999, '9999')");                
                
                executeBatch((String[])list.toArray(new String[list.size()]));
            }
            
            public boolean exceptionExpected() {
                return true;
            }

        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);      

        // now verify the results (all commands will trated as single under single transaction)
        AbstractQueryTest test = new QueryExecution(userTxn.getSource("pm1"));
        test.execute("select * from g1 where e1 >= 400 and e1 < 410");
        test.assertRowCount(0);
        test.execute("select * from g2 where e1 >= 400 and e1 < 410");
        test.assertRowCount(0);      
        test.execute("select * from g2 where e1 = 9999");
        test.assertRowCount(0);                
        test.closeConnection();        
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
    public void testMultipleSourceSelect() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest() {
            public void testCase() throws Exception {
                execute("select * from pm1.g1 join pm2.g1 on pm1.g1.e1 = pm2.g1.e1 where pm1.g1.e1 < 100");
                assertRowCount(100);
            }
            
            public void after() {
                // selects are special case as they will not fail to use multiple sources. The transaction
                // source count only starts when there are updates to db, so this is OK
                if (exceptionOccurred()) {
                    fail("should not have failed to involve multiple sources under optimistic txn");
                }
            }            
        };        
        
        // run test  
        getTransactionContainter().runTransaction(userTxn);
    }
    
    /**
     * Sources = 2
     * Commands = 1, Select
     * Batching = Full Processing, Single Connector Batch
     * result = commit 
     */
    public void testMultipleSourceUpdate() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest() {
            public void testCase() throws Exception {
                Integer value = new Integer(500);
                execute("insert into vm.g1 (pm1e1, pm1e2, pm2e1, pm2e2) values(?,?,?,?)", new Object[] {value, value.toString(), value, value.toString()});
            }
            
            public boolean exceptionExpected() {
                return true;
            }
            
            public void after() {
                if (!exceptionOccurred()) {
                    fail("should have failed to involve multiple sources under optimistic txn");
                }
                else {
                    assertTrue(getLastException().getMessage(), getLastException().getMessage().indexOf("txnAutoWrap=OPTIMISTIC") != -1);
                }                
            }
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);
    }
    
    /**
     * Sources = 2
     * Commands = multiple - Success
     * Batching = Full Processing, Single Connector Batch
     * result = commit
     */
    public void testMultipleSourcesBatchCommand() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest() {
            public void testCase() throws Exception {
                ArrayList list = new ArrayList();                
                list.add("delete from pm1.g2 where pm1.g2.e1 >= 100");
                list.add("delete from pm1.g1 where pm1.g1.e1 >= 100");
                
                list.add("delete from pm2.g2 where pm2.g2.e1 >= 100");
                list.add("delete from pm2.g1 where pm2.g1.e1 >= 100");
                
                for (int i = 200; i < 210; i++) {
                    list.add("insert into pm1.g1 (e1, e2) values("+i+",'"+i+"')");
                    list.add("insert into pm1.g2 (e1, e2) values("+i+",'"+i+"')");
                    
                    list.add("insert into pm1.g1 (e1, e2) values("+i+",'"+i+"')");
                    list.add("insert into pm1.g2 (e1, e2) values("+i+",'"+i+"')");                    
                }
                executeBatch((String[])list.toArray(new String[list.size()]));
            }
            
            public void after() {
                if (!exceptionOccurred()) {
                    fail("should have failed to involve multiple sources under optimistic txn");
                }
                else {
                    assertTrue(getLastException().getMessage(), getLastException().getMessage().indexOf("txnAutoWrap=OPTIMISTIC") != -1);
                }                
            } 
            
            public boolean exceptionExpected() {
                return true;
            }

        };        
        
        getTransactionContainter().runTransaction(userTxn);
    }     
    
    /**
     * Sources = 2
     * Commands = 1, Select
     * Batching = Full Processing, Single Connector Batch
     * result = commit 
     */
    public void testMultipleSourceVirtualSelect() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest() {
            public void testCase() throws Exception {
                execute("select * from vm.g1");
            }
            
            public void after() {
                if (exceptionOccurred()) {
                    fail("should not have failed to involve multiple sources under optimistic txn");
                }
            }            
        };        
        
        getTransactionContainter().runTransaction(userTxn);
    }
    
    /**
     * Sources = 2
     * Commands = 1, Select
     * Batching = Full Processing, Single Connector Batch
     * result = commit 
     */
    public void xtestMultipleSourceVirtualProceduralSelect() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest() {
            public void testCase() throws Exception {
                execute("select * from vm.p1");
            }
            
            public void after() {
                if (exceptionOccurred()) {
                    fail("should have failed to involve multiple sources under optimistic txn");
                }
            }            
        };        
        
        getTransactionContainter().runTransaction(userTxn);
    }   
    
    /**
     * Sources = 2
     * Commands = 1, Select
     * Batching = Full Processing, Single Connector Batch
     * result = commit 
     */
    public void testMultipleSourceVirtualProcedure() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest() {
            public void testCase() throws Exception {
                execute("select * from vm.p2 where vm.p2.e1 = ? and vm.p2.e2 = ?", new Object[] {new Integer(200), "200"});
            }
            
            public boolean exceptionExpected() {
                return true;
            }
            
            public void after() {
                if (!exceptionOccurred()) {
                    fail("should have failed to involve multiple sources under optimistic txn");
                }
                else {
                    assertTrue(getLastException().getMessage(), getLastException().getMessage().indexOf("txnAutoWrap=OPTIMISTIC") != -1);
                }                
            }                 
        };        
        
        getTransactionContainter().runTransaction(userTxn);
    }    
    
    public void testMultipleSourceVirtualProceduralSelectWithUpdate() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest() {
            public void testCase() throws Exception {
                execute("exec vm.p2(?, ?)", new Object[] {new Integer(200), "200"});
            }
            
            public boolean exceptionExpected() {
                return true;
            }
            
            public void after() {
                if (!exceptionOccurred()) {
                    fail("should have failed to involve multiple sources under optimistic txn");
                }
                else {
                    assertTrue(getLastException().getMessage(), getLastException().getMessage().indexOf("txnAutoWrap=OPTIMISTIC") != -1);
                }                
            }                 
        };        
        
        getTransactionContainter().runTransaction(userTxn);
    }    
    
    /**
     * Sources = 2
     * Commands = 1, Select
     * Batching = Full Processing, Single Connector Batch
     * result = commit 
     */
    public void testMultipleSourceVirtualUpdate() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest() {
            public void testCase() throws Exception {
                execute("delete from vm.g1 where vm.g1.pm1e1 > 100");
            }
            
            public void after() {
                if (!exceptionOccurred()) {
                    fail("should have failed to involve multiple sources under optimistic txn");
                }
                else {
                    assertTrue(getLastException().getMessage(), getLastException().getMessage().indexOf("txnAutoWrap=OPTIMISTIC") != -1);
                }
            } 
            
            public boolean exceptionExpected() {
                return true;
            }

        };        
        
        getTransactionContainter().runTransaction(userTxn);
    }     
}
