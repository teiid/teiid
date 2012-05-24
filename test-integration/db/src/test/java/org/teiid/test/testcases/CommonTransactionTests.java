/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.testcases;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Test;
import org.teiid.jdbc.AbstractQueryTest;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.test.framework.query.AbstractQueryTransactionTest;
import org.teiid.test.framework.query.QueryExecution;


/** 
 * A common test case among many different transaction stuff. 
 */
@SuppressWarnings("nls")
public abstract class CommonTransactionTests extends BaseAbstractTransactionTestCase {
    
     
//    void runConcurrentTestCases(int howMany, final String[] sqls) {
//        
//        SeparateClient[] clients = new SeparateClient[howMany]; 
//                       
//        for(int i = 0; i < howMany; i++) {
//            AbstractQueryTransactionTest testCase = new AbstractQueryTransactionTest() {
//                public void testCase() throws Exception {
//                    execute(sqls);
//                }
//            };            
//            clients[i] = new SeparateClient(getTransactionContainter(), testCase);
//        }
//        
//        for(int i = 0; i < howMany; i++) {
//            clients[i].start();
//        }
//
//        try {
//            for(int i = 0; i < howMany; i++) {
//                clients[i].join();
//            }
//        } catch (InterruptedException e) {
//            // boo
//        }        
//    }
    
//    static class SeparateClient extends Thread{
//        TransactionContainer container = null;
//        AbstractTransactionTestCase testCase = null;
//        
//        public SeparateClient(TransactionContainer container, AbstractTransactionTestCase testCase) {
//            this.container = container;
//            this.testCase = testCase;
//        }
//
//        public void run() {
//            this.container.runTransaction(this.testCase);
//        }
//    }
    
    ///////////////////////////////////////////////////////////////////////////////////////////////
    //  Single Source - Rows below 500 (for insert/update/delete)
    ///////////////////////////////////////////////////////////////////////////////////////////////
    
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
        
        // there is nothing to verify here..
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
     * Commands = 1, Update(prepared statement)
     * Batching = Full Processing, Single Connector Batch
     * result = commit 
     */
    @Test
    public void testSingleSourcePreparedUpdate() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testSingleSourcePreparedUpdate") {
            
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
     * Commands = 1, Select
     * Batching = Partial Processing, Single Connector Batch
     * result = commit 
     */
    @Test
    public void testSingleSourcePartialProcessing() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testSingleSourcePartialProcessing") {
            public void testCase() throws Exception {
                execute("select * from pm1.g1 where pm1.g1.e1 < 100 limit 10");
                assertRowCount(10);
            }
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);       
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
     * Commands = 1, Select
     * Batching = Full Processing, Single Connector Batch
     * result = commit 
     */    
    @Test
    public void testMultipleSourceVirtualSelect() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceVirtualSelect") {
            public void testCase() throws Exception {
                execute("select * from vm.g1 where vm.g1.pm1e1 < 100");
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
        test.execute("select * from g1 where e2 = '500'");
        test.assertRowCount(1);
        test.closeConnection();
        
        test = new QueryExecution(userTxn.getSource("pm2"));
        test.execute("select * from g1 where e2 = '500'");
        test.assertRowCount(1);
    }
    
    /**
     * Sources = 2
     * Commands = 1, Update
     * Batching = Full Processing, Single Connector Batch
     * result = commit 
     */
    @Test
    public void testMultipleSourceSelectInto() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceSelectInto") {
            public void testCase() throws Exception {
                execute("insert into vm.g1 (pm1e1, pm1e2, pm2e1, pm2e2) values(501, '501', 501, '501')");
                execute("select pm1.g1.e1, pm1.g1.e2 into pm2.g2 from pm1.g1 where pm1.g1.e1 = 501");
            }
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);       
        
        // now verify the results
        AbstractQueryTest test = new QueryExecution(userTxn.getSource("pm1"));
        test.execute("select * from g1 where e2 = '501'");
        test.assertRowCount(1);
        
        test = new QueryExecution(userTxn.getSource("pm2"));
        test.execute("select * from g1 where e2 = '501'");
        test.assertRowCount(1);
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
            public void testCase() throws Exception {
                for (int i = 100; i < 112; i++) {
                    Integer val = new Integer(i);
                    execute("insert into vm.g1 (pm1e1, pm1e2, pm2e1, pm2e2) values(?,?,?,?)", new Object[] {val, val.toString(), val, val.toString()});
                }
                execute("select pm1.g1.e1, pm1.g1.e2 into pm2.g2 from pm1.g1 where pm1.g1.e1 >= 100");
            }
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);       
        
        // now verify the results
        AbstractQueryTest test = new QueryExecution(userTxn.getSource("pm1"));
        test.execute("select * from g1 where e1 >= 100 and e1 < 112");
        test.assertRowCount(12);
        
        test = new QueryExecution(userTxn.getSource("pm2"));
        test.execute("select * from g1 where e1 >= 100 and e1 < 112");
        test.assertRowCount(12);
        test.execute("select * from g2 where e1 >= 100 and e1 < 112");
        test.assertRowCount(12);        
    }    

    /**
     * Sources = 2
     * Commands = 1, Update(prepared statement)
     * Batching = Full Processing, Single Connector Batch
     * result = commit 
     */
    @Test
    public void testMultipleSourcePreparedUpdate() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourcePreparedUpdate") {
            public void testCase() throws Exception {
                Integer value = new Integer(500);
                execute("insert into vm.g1 (pm1e1, pm1e2, pm2e1, pm2e2) values(?,?,?,?)", new Object[] {value, value.toString(), value, value.toString()});
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
     * result = commit
     */
    @Test
    public void testMultipleSourceMultipleCommands() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceMultipleCommands") {
            public void testCase() throws Exception {
                execute("delete from pm1.g2 where e1 >= ?", new Object[] {new Integer(100)});
                execute("delete from pm1.g1 where e1 >= ?", new Object[] {new Integer(100)});
                execute("delete from pm2.g2 where e1 >= ?", new Object[] {new Integer(100)});
                execute("delete from pm2.g1 where e1 >= ?", new Object[] {new Integer(100)});
                
                execute("select * from pm1.g1");
                assertRowCount(100);
                
                for (int i = 100; i < 115; i++) {
                    Integer val = new Integer(i);
                    execute("insert into pm1.g1 (e1, e2) values(?,?)", new Object[] {val, val.toString()});
                    execute("insert into pm1.g2 (e1, e2) values(?,?)", new Object[] {val, val.toString()});
                    
                    execute("insert into pm2.g1 (e1, e2) values(?,?)", new Object[] {val, val.toString()});
                    execute("insert into pm2.g2 (e1, e2) values(?,?)", new Object[] {val, val.toString()});                    
                }
                
                execute("update pm1.g1 set e2='blah' where e1 > 100");
            }
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);      

        // now verify the results
        AbstractQueryTest test = new QueryExecution(userTxn.getSource("pm1")) {
	    protected boolean compareCaseSensitive() {
		return false;
	    }
        };
        test.execute("select * from g1 where e1 >= 100 and e1 < 115");
        test.assertRowCount(15);
        test.execute("select * from g2 where e1 >= 100 and e1 < 115");
        test.assertRowCount(15);
        test.execute("select distinct e2 from g1 where e1 > 100");
        
        // NOTE:  if this is an oracle source, it failes because it return varchar2
        if (userTxn.getSource("pm1").getMetaData().getDatabaseProductName().toLowerCase().indexOf("oracle") > -1) {
            test.assertResultsSetEquals(new String[] {"e2[varchar2]", "blah"});
        } else {
            test.assertResultsSetEquals(new String[] {"e2[varchar]", "blah"});
        }
     
    }
    
    /**
     * Sources = 2
     * Commands = multiple - Success
     * Batching = Full Processing, Single Connector Batch
     * result = commit
     */
    @Test
    public void testMultipleSourceMultipleVirtualCommands() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceMultipleVirtualCommands") {
            public void testCase() throws Exception {

                for (int i = 200; i < 207; i++) {
                    Integer val = new Integer(i);
                    execute("insert into vm.g1 (pm1e1, pm1e2, pm2e1, pm2e2) values(?,?,?,?)", new Object[] {val, val.toString(), val, val.toString()});
                    execute("insert into vm.g2 (pm1e1, pm1e2, pm2e1, pm2e2) values(?,?,?,?)", new Object[] {val, val.toString(), val, val.toString()});                    
                }
                
                execute("update vm.g1 set pm1e2='blah' where pm1e1 >= 200");
                
                execute("delete from vm.g2 where vm.g2.pm1e1 >= 205");
                execute("delete from vm.g1 where vm.g1.pm1e1 >= 205");
                
                execute("select * from vm.g1 where pm1e1 >= 200 and pm1e1 < 207");
                assertRowCount(5);
            }
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);      

        // now verify the results
        AbstractQueryTest test = new QueryExecution(userTxn.getSource("pm1")){
	    protected boolean compareCaseSensitive() {
		return false;
	    }
        };
        test.execute("select * from g1 where e1 >= 200 and e1 < 207");
        test.assertRowCount(5);
        test.execute("select * from g2 where e1 >= 200 and e1 < 207");
        test.assertRowCount(5);
        test.execute("select distinct e2 from g1 where e1 >= 200 and e1 < 207");
        test.assertResultsSetEquals(new String[] {"e2[varchar2]", "blah"});
      
    }    
        
    /**
     * Sources = 2
     * Commands = multiple - Success
     * Batching = Full Processing, Single Connector Batch
     * result = rollback
     */    
    @Test
    public void testMultipleSourceMultipleCommandsCancel() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceMultipleCommandsCancel") {
            
            public void testCase() throws Exception {
                Thread t = new Thread("Cancel Thread") {
                    public void run() {
                        try {
                            try {
                                Thread.sleep(500);
                                cancelQuery();
                            } catch (SQLException e) {
                         //       debug(e.getMessage());
                            }
                        } catch (InterruptedException e) {}
                    }
                };
                t.start();
                executeBatch(getMultipleSourceBatch());
            }
           

            public boolean exceptionExpected() {
                return true;
            }
        };
        getTransactionContainter().runTransaction(userTxn);
                
        // now verify the results (this may finish under one second, then this test is not valid)
        AbstractQueryTest test = new QueryExecution(userTxn.getSource("pm1"));
        test.execute("select * from g1 where e1 >= 600 and e1 < 650");
        test.assertRowCount(0);
        test.execute("select * from g2 where e1 >= 600 and e1 < 650");
        test.assertRowCount(0);
        test.execute("select distinct e2 from g1 where e1 >= 600 and e1 < 650");
        test.assertRowCount(0);
    }

    /**
     * Sources = 2
     * Commands = multiple - Success
     * Batching = Full Processing, Single Connector Batch
     * result = rollback
     */    
    @Test
    public void testMultipleSourceTimeout() throws Exception{
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceTimeout") {
            public void testCase() throws Exception {
                executeBatch(getMultipleSourceBatch(), 1); // time out after 1 sec
            }
            
            public boolean exceptionExpected() {
                return true;
            }            
            
            public void after() {
                if (!exceptionOccurred()) {
                   	Assert.assertTrue("should have failed with time out exception", false );
                }
                else {
                    if (getLastException() != null) {
                                                        
                	String msg = "NA";
                 	SQLException s = getLastException();
                 	
                 	Throwable t = s.getCause();
                 	if (t instanceof TimeoutException) {
                 	   msg = t.getMessage();
                 	} else if (s instanceof TeiidSQLException) {
                 		 TeiidSQLException mm = (TeiidSQLException) t;
                 	     if (mm.getNextException() != null) {
                 		 SQLException next = mm.getNextException();
                 		 msg = next.getMessage();
                 	     } else {
                 		 msg = mm.getMessage();
                 	     }
                 	} else {
                 	
                 	    msg = s.getMessage();
                 	}
                 	boolean isfound = (msg.indexOf("Operation timed out before completion") != -1 ? true : false);

                 	Assert.assertTrue("Exception Message didnt match 'Operation timed out before completion' found: " + msg, isfound );
                    } else {
                	Assert.assertTrue("Program Error: it indicates exception occured, but no exception is found", false );
                    }
                }
            }             
        };
        getTransactionContainter().runTransaction(userTxn);
        
        // now verify the results (this may finish under one second, then this test is not valid)
        AbstractQueryTest test = new QueryExecution(userTxn.getSource("pm1"));
        test.execute("select * from g1 where e1 >= 600 and e1 < 750");
        test.assertRowCount(0);
        test.execute("select * from g2 where e1 >= 600 and e1 < 750");
        test.assertRowCount(0);
        test.execute("select distinct e2 from g1 where e1 >= 600 and e1 < 750");
        test.assertRowCount(0);
      
    }    
    
        
    static String[] getMultipleSourceBatch() {
        ArrayList list = new ArrayList();
        
        for (int i = 600; i < 750; i++) {
            list.add("insert into pm1.g1 (e1, e2) values("+i+",'"+i+"')");
            list.add("insert into pm1.g2 (e1, e2) values ("+i+",'"+i+"')");
            list.add("insert into pm2.g1 (e1, e2) values("+i+",'"+i+"')");
            list.add("insert into pm2.g2 (e1, e2) values ("+i+",'"+i+"')");                                
        }
        
        list.add("update pm1.g1 set e2='blah' where pm1.g1.e1 >= 600");
        list.add("update pm2.g1 set e2='blah' where pm2.g1.e1 >= 600");
        
        list.add("delete from pm1.g2 where pm1.g2.e1 >= 610");
        list.add("delete from pm1.g1 where pm1.g1.e1 >= 610");
        list.add("delete from pm2.g2 where pm2.g2.e1 >= 610");
        list.add("delete from pm2.g1 where pm2.g1.e1 >= 610");
        
        return(String[])list.toArray(new String[list.size()]);
    }
    
    
    /**
     * Sources = 2
     * Commands = 1, Select
     * Batching = Partial Processing, Single Connector Batch
     * result = commit 
     * Note: This is producing the below error some times; however this is SQL Server issue.
     * http://support.microsoft.com/?kbid=834849
     */
    @Test
    public void testMultipleSourcePartialProcessingUsingLimit() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourcePartialProcessingUsingLimit") {
            public void testCase() throws Exception {
                execute("select * from vm.g1 where pm1e1 < 100 limit 10");
                assertRowCount(10);
            }
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);       
    }  

    /**
     * Sources = 2
     * Commands = 1, Select
     * Batching = Partial Processing, Single Connector Batch
     * result = commit
     * Note: This is producing the below error some times; however this is SQL Server issue.
     * http://support.microsoft.com/?kbid=834849
     */
    @Test
    public void testMultipleSourcePartialProcessingUsingMakedep() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourcePartialProcessingUsingMakedep") {
            public void testCase() throws Exception {
                execute("select pm1.g1.e1, pm1.g1.e2 from pm1.g1 LEFT OUTER JOIN pm2.g1 MAKENOTDEP ON pm1.g1.e2 = pm2.g1.e2 where pm2.g1.e1 >= 50 and pm2.g1.e1 < 100");
                assertRowCount(50);
            }
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);       
    }        
}
