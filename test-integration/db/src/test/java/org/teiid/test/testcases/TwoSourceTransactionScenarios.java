/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.testcases;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import org.teiid.test.framework.ConfigPropertyNames;
import org.teiid.test.framework.query.AbstractQueryTransactionTest;
import org.teiid.test.framework.query.QueryExecution;

import com.metamatrix.jdbc.api.AbstractQueryTest;



/** 
 * Test cases that require 2 datasources 
 */
public class TwoSourceTransactionScenarios extends SingleSourceTransactionScenarios {
    
    public TwoSourceTransactionScenarios(String name) {
        super(name);
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
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceSelect") {
            public void testCase() throws Exception {
                execute("select * from pm1.g1 join pm2.g1 on pm1.g1.e1 = pm2.g1.e1 where pm1.g1.e1 < 100");
                assertRowCount(100);
            }
            
            public int getNumberRequiredDataSources(){
            	return 2;
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
    public void testMultipleSourceViewSelect() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceViewSelect") {
            public void testCase() throws Exception {
                execute("select * from vm.g1 where vm.g1.pm1e1 < 100");
                assertRowCount(100);
            }
            public int getNumberRequiredDataSources(){
            	return 2;
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
    public void testMultipleSourceViewUpdate() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceViewUpdate") {
            public void testCase() throws Exception {
                execute("insert into vm.g1 (pm1e1, pm1e2, pm2e1, pm2e2) values(500, '500', 500, '500')");
            }
            
            public int getNumberRequiredDataSources(){
            	return 2;
            }
             
         	public void validateTestCase() throws Exception {

                // now verify the results
                AbstractQueryTest test = new QueryExecution(getSource("pm1"));
                test.execute("select * from g1 where e2 = '500'");
                test.assertRowCount(1);
                test.closeConnection();
                
                test = new QueryExecution(getSource("pm2"));
                test.execute("select * from g1 where e2 = '500'");
                test.assertRowCount(1);
                test.closeConnection();
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
    public void testMultipleSourceViewSelectInto() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceViewSelectInto") {
            public void testCase() throws Exception {
                execute("insert into vm.g1 (pm1e1, pm1e2, pm2e1, pm2e2) values(501, '501', 501, '501')");
                execute("select pm1.g1.e1, pm1.g1.e2 into pm2.g2 from pm1.g1 where pm1.g1.e1 = 501");
            }
            
            public int getNumberRequiredDataSources(){
            	return 2;
            }
            
         	public void validateTestCase() throws Exception {
                
                // now verify the results
                AbstractQueryTest test = new QueryExecution(getSource("pm1"));
                test.execute("select * from g1 where e2 = '501'");
                test.assertRowCount(1);
                test.closeConnection();
                
                test = new QueryExecution(getSource("pm2"));
                test.execute("select * from g1 where e2 = '501'");
                test.assertRowCount(1);
                test.closeConnection();
                
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
    public void testMultipleSourceViewBulkRowInsert() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceViewBulkRowInsert") {
            public void testCase() throws Exception {
                for (int i = 100; i < 112; i++) {
                    Integer val = new Integer(i);
                    execute("insert into vm.g1 (pm1e1, pm1e2, pm2e1, pm2e2) values(?,?,?,?)", new Object[] {val, val.toString(), val, val.toString()});
                }
                execute("select pm1.g1.e1, pm1.g1.e2 into pm2.g2 from pm1.g1 where pm1.g1.e1 >= 100");
            }
            
            public int getNumberRequiredDataSources(){
            	return 2;
            }
          	public void validateTestCase() throws Exception {
                
                // now verify the results
          		Connection ds = getSource("pm1");
          		System.out.println("Datasource: " + ds.getMetaData().getDatabaseProductName());
                AbstractQueryTest test = new QueryExecution(ds);
                test.execute("select * from g1 where e1 >= 100 and e1 < 112");
                test.assertRowCount(12);
                test.closeConnection();
                
                test = new QueryExecution(getSource("pm2"));
                test.execute("select * from g1 where e1 >= 100 and e1 < 112");
                test.assertRowCount(12);
                test.execute("select * from g2 where e1 >= 100 and e1 < 112");
                test.assertRowCount(12);        
                test.closeConnection();
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
    public void testMultipleSourceViewBulkRowInsertRollback() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceViewBulkRowInsertRollback") {
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
            
            public int getNumberRequiredDataSources(){
            	return 2;
            }
 
            
         	public void validateTestCase() throws Exception {
                // now verify the results
                AbstractQueryTest test = new QueryExecution(getSource("pm1"));
                test.execute("select * from g1 where e1 >= 100 and e1 < 120");
                test.assertRowCount(0);
                test.closeConnection();
                
                test = new QueryExecution(getSource("pm2"));
                test.execute("select * from g1 where e1 >= 100 and e1 < 120");
                test.assertRowCount(0);
                test.execute("select * from g2 where e1 >= 100 and e1 < 120");
                test.assertRowCount(0);        
                test.closeConnection();
         	}
 
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);       
 
    } 


    /**
     * Sources = 2
     * Commands = 1, Update(prepared statement)
     * Batching = Full Processing, Single Connector Batch
     * result = commit 
     */
    public void testMultipleSourceViewPreparedUpdate() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceViewPreparedUpdate") {
            public void testCase() throws Exception {
                Integer value = new Integer(500);
                execute("insert into vm.g1 (pm1e1, pm1e2, pm2e1, pm2e2) values(?,?,?,?)", new Object[] {value, value.toString(), value, value.toString()});
            }
            public int getNumberRequiredDataSources(){
            	return 2;
            }
             
         	public void validateTestCase() throws Exception {
                
                // now verify the results
                AbstractQueryTest test = new QueryExecution(getSource("pm1"));
                test.execute("select * from g1 where e1 = 500");
                test.assertRowCount(1);
                test.closeConnection();
                
                test = new QueryExecution(getSource("pm2"));
                test.execute("select * from g1 where e1 = 500");
                test.assertRowCount(1);
                test.closeConnection();  
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
    public void lookat_testMultipleSourceMultipleCommands() throws Exception {
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
            public int getNumberRequiredDataSources(){
            	return 2;
            }
            
            // because different databases return "varchar" in all caps "VARCHAR"
            // the comparison is being done in a noncasesensitive manner
            public boolean compareResultsCaseSensitive() {
            	return false;
            }
             
         	public void validateTestCase() throws Exception {

                // now verify the results
                AbstractQueryTest test = new QueryExecution(getSource("pm1"));
                test.execute("select * from g1 where e1 >= 100 and e1 < 115");
                test.assertRowCount(15);
                test.execute("select * from g2 where e1 >= 100 and e1 < 115");
                test.assertRowCount(15);
                test.execute("select distinct e2 from g1 where e1 > 100");
                
 //               assertResultsSetEquals(this.internalResultSet., new String[] {"e2[varchar]", "blah"});
                
                test.assertResultsSetEquals(new String[] {"e2[varchar]", "blah"});
                test.closeConnection();  
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
    public void testMultipleSourceViewMultipleCommands() throws Exception {
 //   	this.setAssignModelToDatabaseType("pm1", DataSourceFactory.DataBaseTypes.);
    	
    	this.addProperty(ConfigPropertyNames.EXCLUDE_DATASBASE_TYPES_PROP, "oracle");
    	
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceViewMultipleCommands") {
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
            public int getNumberRequiredDataSources(){
            	return 2;
            }
            
         	public void validateTestCase() throws Exception {
                // now verify the results
                AbstractQueryTest test = new QueryExecution(getSource("pm1"));
                test.execute("select * from g1 where e1 >= 200 and e1 < 207");
                test.assertRowCount(5);
                test.execute("select * from g2 where e1 >= 200 and e1 < 207");
                test.assertRowCount(5);
                test.execute("select distinct e2 from g1 where e1 >= 200 and e1 < 207");
                test.assertResultsSetEquals(new String[] {"e2[varchar]", "blah"});
                test.closeConnection();  
                
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
    public void testMultipleSourceViewMultipleCommandsRollback() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceViewMultipleCommandsRollback") {
            public void testCase() throws Exception {

                for (int i = 600; i < 615; i++) {
                    Integer val = new Integer(i);
                    execute("insert into vm.g1 (pm1e1, pm1e2, pm2e1, pm2e2) values(?,?,?,?)", new Object[] {val, val.toString(), val, val.toString()});
                    execute("insert into vm.g2 (pm1e1, pm1e2, pm2e1, pm2e2) values(?,?,?,?)", new Object[] {val, val.toString(), val, val.toString()});                    
                }
                
                execute("select * from vm.g1 where pm1e1 >= 600 and pm1e1 < 615");
                assertRowCount(15);

                
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
            
            public int getNumberRequiredDataSources(){
            	return 2;
            }
 
            
         	public void validateTestCase() throws Exception {
                // now verify the results
                AbstractQueryTest test = new QueryExecution(getSource("pm1"));
                test.execute("select * from g1 where e1 >= 600 and e1 < 615");
                test.assertRowCount(0);
                test.execute("select * from g2 where e1 >= 600 and e1 < 615");
                test.assertRowCount(0);
                test.execute("select distinct e2 from g1 where e1 >= 600 and e1 < 615");
                test.assertRowCount(0);
                test.closeConnection();   
         	}
 
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);      

    }    

        
    /**
     * Sources = 2
     * Commands = multiple - Success
     * Batching = Full Processing, Single Connector Batch
     * result = rollback
     */    
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
                            	print(e);
                            //    debug(e.getMessage());
                            }
                        } catch (InterruptedException e) {}
                    }
                };
                t.start();
                executeBatch(getMultipleSourceBatch());
            }
           
            /** 
             * @see com.metamatrix.transaction.test.framework.AbstractQueryTest#exceptionExpected()
             */
            public boolean exceptionExpected() {
                return true;
            }
            
            public int getNumberRequiredDataSources(){
            	return 2;
            }
 
            
         	public void validateTestCase() throws Exception {
                // now verify the results (this may finish under one second, then this test is not valid)
                AbstractQueryTest test = new QueryExecution(getSource("pm1"));
                test.execute("select * from g1 where e1 >= 600 and e1 < 650");
                test.assertRowCount(0);
                test.execute("select * from g2 where e1 >= 600 and e1 < 650");
                test.assertRowCount(0);
                test.execute("select distinct e2 from g1 where e1 >= 600 and e1 < 650");
                test.assertRowCount(0);
                test.closeConnection(); 
                
          	}
 
        };
        getTransactionContainter().runTransaction(userTxn);

    }
    
    
    /**
     * Sources = 2
     * Commands = multiple - Success
     * Batching = Full Processing, Single Connector Batch
     * result = rollback
     */
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
            
            public boolean exceptionExpected() {
                return true;
            }
            
            public int getNumberRequiredDataSources(){
            	return 2;
            }
 
            
         	public void validateTestCase() throws Exception {
                // now verify the results
                AbstractQueryTest test = new QueryExecution(getSource("pm1"));
                test.execute("select * from g1 where e1 >= 700 and e1 < 720");
                test.assertRowCount(0);        
                test.closeConnection();
                
                test = new QueryExecution(getSource("pm2"));
                test.execute("select * from g1 where e1 >= 700 and e1 < 720");
                test.assertRowCount(0);        
                test.closeConnection();    
                
         	}
 
            
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);      

    }
    
    /**
     * Sources = 2
     * Commands = multiple - Success
     * Batching = Full Processing, Single Connector Batch
     * result = rollback
     */
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
            
            public int getNumberRequiredDataSources(){
            	return 2;
            }
 
            
         	public void validateTestCase() throws Exception {
                // now verify the results
                AbstractQueryTest test = new QueryExecution(getSource("pm1"));
                test.execute("select * from g1 where e1 >= 700 and e1 < 720");
                test.assertRowCount(0);
                test.closeConnection();        
                
                test = new QueryExecution(getSource("pm2"));
                test.execute("select * from g1 where e1 >= 700 and e1 < 720");
                test.assertRowCount(0);        
                test.closeConnection();   
         	}
 
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);      

    }



    /**
     * Sources = 2
     * Commands = multiple - Success
     * Batching = Full Processing, Single Connector Batch
     * result = rollback
     */    
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
                    fail("should have failed with time out exception");
                }
                else if (getLastException() != null){
                	if (getLastException().getMessage().indexOf("Operation timed out before completion") != -1) {
                		assertTrue(false);
                	}
                } else {
                	fail("The expected exception was not saved.");
                }
            } 
            
            public int getNumberRequiredDataSources(){
            	return 2;
            }
 
            
         	public void validateTestCase() throws Exception {
                // now verify the results (this may finish under one second, then this test is not valid)
                AbstractQueryTest test = new QueryExecution(getSource("pm1"));
                test.execute("select * from g1 where e1 >= 600 and e1 < 750");
                test.assertRowCount(0);
                test.execute("select * from g2 where e1 >= 600 and e1 < 750");
                test.assertRowCount(0);
                test.execute("select distinct e2 from g1 where e1 >= 600 and e1 < 750");
                test.assertRowCount(0);
                test.closeConnection();   
         	}
 
        };
        getTransactionContainter().runTransaction(userTxn);
 
    }    
    
        
    static String[] getMultipleSourceBatch() {
        ArrayList<String> list = new ArrayList<String>();
        
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
    public void testMultipleSourceViewPartialProcessingUsingLimit() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourceViewPartialProcessingUsingLimit") {
            public void testCase() throws Exception {
                execute("select * from vm.g1 where pm1e1 < 100 limit 10");
                assertRowCount(10);
            }
            
            public int getNumberRequiredDataSources(){
            	return 2;
            }
 
            
         	public void validateTestCase() throws Exception {
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
    public void testMultipleSourcePartialProcessingUsingMakedep() throws Exception {
        AbstractQueryTransactionTest userTxn = new AbstractQueryTransactionTest("testMultipleSourcePartialProcessingUsingMakedep") {
            public void testCase() throws Exception {
                execute("select pm1.g1.e1, pm1.g1.e2 from pm1.g1 LEFT OUTER JOIN pm2.g1 MAKENOTDEP ON pm1.g1.e2 = pm2.g1.e2 where pm2.g1.e1 >= 50 and pm2.g1.e1 < 100");
                assertRowCount(50);
            }
            
            public int getNumberRequiredDataSources(){
            	return 2;
            }
 
            
         	public void validateTestCase() throws Exception {
          	}
 
        };        
        
        // run test
        getTransactionContainter().runTransaction(userTxn);  
 
    }        
    
     
     

    
    

}
