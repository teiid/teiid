/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.datasource;

import java.sql.Connection;
import java.sql.Statement;

import org.teiid.test.framework.connection.ConnectionStrategy;
import org.teiid.test.framework.exception.QueryTestFailedException;

/** 
 * This class loads the data in the databases specified, to a known state
 */
@SuppressWarnings("nls")
public class DataStore {
    
     /**
     * Called at the start of all the tests to initialize the database to ensure
     * it's in the proper state.
     * 
     * @param connStrategy
     */
    public static void initialize(ConnectionStrategy connStrategy) {
	
	if (connStrategy.isDataStoreDisabled()) {
	    return;
	}
	try {
	    load(getConnection("pm1", connStrategy));
	    
	    load(getConnection("pm2", connStrategy));

        } catch (Exception e) {
            throw new RuntimeException(e);
        } 
    }
    
    private static Connection getConnection(String identifier, ConnectionStrategy connStrategy) throws QueryTestFailedException {
	Connection conn = connStrategy.createDriverConnection(identifier);
	// force autocommit back to true, just in case the last user didnt
	try {
		conn.setAutoCommit(true);
	} catch (Exception sqle) {
		throw new QueryTestFailedException(sqle);
	}
	
	return conn;
    }
    
    private static void load(Connection c) throws Exception {
        // DDL
        // drop table g1;
        // drop table g2;
        
        // oracle
        // create Table g1 (e1 number(5) PRIMARY KEY,   e2 varchar2(50));
        // create Table g2 (e1 number(5) REFERENCES g1, e2 varchar2(50));
        
        // SQL Server
        // create Table g1 (e1 int PRIMARY KEY,   e2 varchar(50));
        // create Table g2 (e1 int references g1, e2 varchar(50));   
	            

            Statement stmt = c.createStatement();
            try {        	
                stmt.execute("delete from g2");
                stmt.execute("delete from g1");
                
                for (int i = 0; i < 100; i++) {
                    stmt.execute("insert into g1 (e1, e2) values("+i+",'"+i+"')");
                }
                
                
                for (int i = 0; i < 50; i++) {
                    stmt.execute("insert into g2 (e1, e2) values("+i+",'"+i+"')");
                }
            
            } finally {
        	stmt.close();
            }

    }
    
    /**
     * Called as part of the setup for each test.  
     * This will set the database state as if {@link #initialize(ConnectionStrategy)} was called.
     * However, for performance reasons, the process goes about removing what's not needed instead of cleaning out everything
     * and reinstalling. 
     * @param connStrategy
     */
    public static void setup(ConnectionStrategy connStrategy) {
	if (connStrategy.isDataStoreDisabled()) {
	    return;
	}
	try {
	    setUpTest(getConnection("pm1", connStrategy));
	    
	    setUpTest(getConnection("pm2", connStrategy));

        } catch (Exception e) {
            throw new RuntimeException(e);
        } 

	
    }
    
    private static void setUpTest(Connection c) throws Exception {
	
            Statement stmt = c.createStatement();
            try {
                stmt.execute("delete from g2 where e1 >= 50"); //$NON-NLS-1$
                stmt.execute("delete from g1 where e1 >= 100"); 

            } finally {
        	stmt.close();
            }
            

    }

}
