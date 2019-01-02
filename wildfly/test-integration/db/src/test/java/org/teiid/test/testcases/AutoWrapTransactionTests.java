/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.testcases;

import java.util.ArrayList;

import org.junit.Test;
import org.teiid.jdbc.AbstractQueryTest;
import org.teiid.test.framework.ConfigPropertyNames;
import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.query.AbstractQueryTransactionTest;
import org.teiid.test.framework.query.QueryExecution;
import org.teiid.test.framework.transaction.TxnAutoTransaction;

@SuppressWarnings("nls")
public class AutoWrapTransactionTests extends CommonTransactionTests {

    
    protected TransactionContainer getTransactionContainter() {

	return new TxnAutoTransaction(ConfigPropertyNames.TXN_AUTO_WRAP_OPTIONS.AUTO_WRAP_AUTO);
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
                
                // try to rollback, however since this pessimistic above two are already commited
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
        test.execute("select * from g1 where e1 >= 200 and e1 < 210");
        test.assertRowCount(10);
        test.execute("select * from g2 where e1 = 9999");
        test.assertRowCount(0);        
      
    }
    
    /**
     * Sources = 1
     * Commands = multiple - Success
     * Batching = Full Processing, Single Connector Batch
     * result = rollback
     */
    @Test
    public void testSingleSourceBatchCommandReferentialIntegrityRollback() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testSingleSourceBatchCommandReferentialIntegrityRollback") {
            public void testCase() throws Exception {
                ArrayList list = new ArrayList();
                for (int i = 200; i < 210; i++) {
                    list.add("insert into pm1.g1 (e1, e2) values("+i+",'"+i+"')");
                }
                
                // try to rollback, since we are in single batch it must rollback
                list.add("insert into pm1.g2 (e1, e2) values(9999,'9999')");
                executeBatch((String[])list.toArray(new String[list.size()]));
            }
            
            public boolean exceptionExpected() {
                return true;
            }            
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);      

        // now verify the results
        AbstractQueryTest test = new QueryExecution(userTxn.getSource("pm1"));
        test.execute("select * from g1 where e1 >= 200 and e1 < 210");
        test.assertRowCount(0);
        test.execute("select * from g2 where e1 = 9999");
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
            ArrayList list = new ArrayList();
            public void testCase() throws Exception {
                for (int i = 100; i < 120; i++) {
                    list.add("insert into vm.g1 (pm1e1, pm1e2, pm2e1, pm2e2) values("+i+",'"+i+"',"+i+",'"+i+"')");
                }
                list.add("select pm1.g1.e1, pm1.g1.e2 into pm2.g2 from pm1.g1 where pm1.g1.e1 >= 100");
                
                // force the rollback by trying to insert an invalid row.
                list.add("insert into pm1.g2 (e1, e2) values(9999,'9999')");
                
                executeBatch((String[])list.toArray(new String[list.size()]));
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
}
