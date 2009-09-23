/*
 * Copyright 2000-2008 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.rhq.comm.impl;

import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.teiid.rhq.comm.Connection;
import org.teiid.rhq.comm.ConnectionConstants;
import org.teiid.rhq.comm.ConnectionPool;
//import org.teiid.rhq.embedded.pool.ConnectionPoolImpl;

import org.teiid.rhq.enterprise.pool.ConnectionPoolImpl;




/** 
 * @since 4.3
 */
public class TestConnectionPool extends TestCase  {
    static {
 //       System.setProperty(SingletonConnectionManager.INSTALL_SERVER_PROP,StartingEnvironmentConstants.SINGLE_SYSTEM_PARM);
        
        // reset the conn mager for the next test case class
        
    }    
    
    private boolean failure = false;
    int count;
    
    /**
     * This suite of all tests could be defined in another class but it seems easier to 
     * maintain it here.
     */
    public static Test suite() {

        TestSuite suite = new TestSuite("TestConnectionPool"); //$NON-NLS-1$
        suite.addTestSuite(TestConnectionPool.class); 
        return suite;
    }


    // ################################## MAIN ################################

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }    

    /**
     * @since 1.0
     */
//    public static void main(final String[] arguments) {
//        try {
//            
//            TestConnectionPool test = new TestConnectionPool();
//            test.runTest();
//
//            
//        }catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
    
//    public void runTest() throws Exception {
//        testSimple();
//        testSecond();
//        testThreading();
//    }
    
    public void testSimple() throws Exception {
        log("Start simpleTest...");
        failure = false;
        
        Properties props = new Properties();
        props.setProperty(ConnectionPool.CONNECTION_FACTORY, "org.teiid.rhq.comm.impl.FakeConnectionFactory");
        
        ConnectionPool pool = new ConnectionPoolImpl();
        pool.initialize(props, this.getClass().getClassLoader());
        
        if (!FakeConnectionFactory.createdfactory) {
            logError("Didn't use the fake factory");
        }
        
        Connection conn = pool.getConnection();
        
        int cnt = pool.getAvailableConnectionCount();
        if (cnt != 0) {
            logError("Available count should have been 0, but was " + cnt);
        }
        
        
        cnt = pool.getConnectionsInUseCount();
        if (cnt != 1) {
            logError("Connections in use should have been 1, but was " + cnt);
        }    

          pool.close(conn);
        
        cnt = pool.getConnectionsInUseCount();
        if (cnt != 0) {
            logError("Connections in use should have been 0 after checkin, but was " + cnt);
        } 
        
        cnt = pool.getAvailableConnectionCount();
        if (cnt != 1) {
            logError("Available count should have been 1 after checking, but was " + cnt);
        }       
        
        pool.shutdown();
        log("Was simpleTest a success " + !failure );
    }
    
    public void testSecond() throws Exception {
        log("Start secondTest...");
        failure = false;
       Properties props = new Properties();
        props.setProperty(ConnectionPool.CONNECTION_FACTORY, "org.teiid.rhq.comm.impl.FakeConnectionFactory");
        
        ConnectionPool pool = new ConnectionPoolImpl();
        pool.initialize( props, this.getClass().getClassLoader());
        
        if (!FakeConnectionFactory.createdfactory) {
            logError("Didn't use the fake factory");
        }        
        
        Connection conn = pool.getConnection();
        Connection conn2 = pool.getConnection();
        Connection conn3 = pool.getConnection();
        
        validateAvailCnt(pool, 0);

        validateInUseCnt(pool, 3);
        

        pool.close(conn);

        validateAvailCnt(pool, 1);

        validateInUseCnt(pool, 2);
        
        
        Connection conn4 = pool.getConnection();
        Connection conn5 = pool.getConnection();
        
        validateAvailCnt(pool, 0);

        validateInUseCnt(pool, 4);
        
        pool.close(conn5);
        pool.close(conn3);
        pool.close(conn2);
        pool.close(conn4);
        
        validateAvailCnt(pool, 4);

        validateInUseCnt(pool, 0);
        
        pool.shutdown();
        log("Was secondTest a success " + !failure );
    }  
    
    private void validateInUseCnt(ConnectionPool pool, int cnt) {
        
//        int incnt = pool.getConnectionsInUseCount();
//        if (incnt != cnt) {
//            logError("Connections in use should have been " + cnt + " , but was " + incnt);
//        }   
    }
    
    private void validateAvailCnt(ConnectionPool pool, int cnt) {
        
//        int incnt = pool.getAvailableConnectionCount();
//        if (incnt != cnt) {
//            logError("Available count should have been " + cnt + " , but was " + incnt);
//        }   
    }    
    
    public void testThreading() throws Exception {
        failure = false;
        log("Start threadingTest...");
        
        Properties props = new Properties();
        props.setProperty(ConnectionPool.CONNECTION_FACTORY, "org.teiid.rhq.comm.impl.FakeConnectionFactory");
        props.setProperty(ConnectionConstants.PASSWORD, "PW");
        props.setProperty(ConnectionConstants.USERNAME, "USERNAME");
                
        ConnectionPool pool = new ConnectionPoolImpl();
        pool.initialize( props, this.getClass().getClassLoader());
        
        
        int threadCnt = 10;
//        int max_size = 1;
 //       int expectedNumErrors = threadCnt - max_size;

        TimeOutThread[] ts = new TimeOutThread[threadCnt];

        for (count = 0; count < threadCnt; count++) {
            ts[count] = new TimeOutThread(pool, 10, count);
        }
    
        for(int k = 0; k < threadCnt; k++){
                    ts[k].start();
        }
    
          try {
                for(int k = 0; k < threadCnt; k++){
                    ts[k].join();
                }
          } catch (InterruptedException e) {
          }
    
          for(int k = 0; k < threadCnt; k++){
            if (ts[k].hasException()) {
                Exception e = ts[k].getException();

                logError("Exception " + e.getMessage());

    
            }
            // which thread
            int pc = ts[k].getProcessCnt();
            
            // number of connections suppose to process per thread
            int ptc = ts[k].getPerThreadCnt();

            // the number of connection actually processed
            int mx = ts[k].getMaxProcessedCnt();
            
            // the end count of the connection left after close is suppose to be called
            int ct = ts[k].getConnectionCount();
            if (ct != 0) {
                logError("Thread " + pc + " did have the connections go back down to zero");
            }
            if (ptc != mx) {
                logError("PerThreadCnt " + ptc + ", but only successfully processed " + mx);
            } else {
                log("Process " + pc + " for thread # " + k);
            }
            
            
          }
    
          log("Was threadingTest a success " + !failure );
   

    }


    
    private void logError(String msg) {
        failure = true;
        System.out.println(msg);
    }

    private void log(String msg) {
        System.out.println(msg);
    }    
    
    protected  class TimeOutThread extends BaseThread{
      private ConnectionPool pool;
      private int connCnt = 0;
      private int processCnt = 0;
      private int maxCnt = 0;


      public TimeOutThread(ConnectionPool connpool, int connections, int processcnt) {
          super(connections);
          this.pool = connpool;
          this.processCnt = processcnt;
          

      }
      public int getProcessCnt() {
          return processCnt;
      }
      
      public int getMaxProcessedCnt() {
          return maxCnt;
      }
      public int getConnectionCount() {
          return this.connCnt;
      }
      
        public void run(){
      // DO NOT call resource.close(), all the resources should remain
      // checkedout to cause the next resource request to timeout.
      

        for (int i=0; i < perThreadCnt; i++ ) {
            Connection conn = null;
            try {
                conn = pool.getConnection();
                ++connCnt;
                ++maxCnt;
                
                 yield();
//               Properties psrops =conn.
//                if (psrops == null || psrops.isEmpty()) {
//                    setException(new Exception("Null Environment"));
//                }
//                if (conn.getProperty(ConnectionConstants.USERNAME) == null) {
//                    setException(new Exception("No UserName"));
//                }
//                if (psrops.size() < 3) {
//                    setException(new Exception("NOt Enough Properties"));
//                    System.out.println(psrops);
//                }

            } catch (Exception toe) {
                setException(toe);

            }

            // yield to the other thread to checkout instance, which should timeout
            try {
                pool.close(conn);
                --connCnt;
            } catch (Exception err) {
                setException(err);
            }
        }
        }   
    }

    protected class BaseThread extends Thread{
        protected String objName = "Thread " + count; //$NON-NLS-1$
      protected int perThreadCnt = 1;
      private Exception t = null;


      public BaseThread(int iterationCnt) {
          perThreadCnt = iterationCnt;
      }
      
      public int getPerThreadCnt() {
          return perThreadCnt;
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
