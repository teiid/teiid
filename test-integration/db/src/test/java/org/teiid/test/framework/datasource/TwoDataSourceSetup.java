/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.datasource;

import org.teiid.test.framework.DataSourceSetup;
import org.teiid.test.framework.QueryExecution;
import org.teiid.test.framework.connection.ConnectionUtil;

import com.metamatrix.jdbc.api.AbstractQueryTest;



/** 
 * This performs the data setup for SingleSource test cases  
 */
public class TwoDataSourceSetup implements DataSourceSetup {
    
   
	@Override
	public void setup() throws Exception {
    // NOTE:  dont close the connections here because in most cases they are reused
    //			to validate the results
    //		The connections will be closed at teardown
    	
     	
       System.out.println("Run TwoSource Setup...");

    	
        AbstractQueryTest test1 = new QueryExecution(ConnectionUtil.getSource("pm1")); //$NON-NLS-1$
        test1.execute("delete from g2"); //$NON-NLS-1$
        test1.execute("delete from g1");         //$NON-NLS-1$
        
        test1.execute("select * from g1 ");
        test1.assertRowCount(0);
        test1.execute("select * from g2 ");
        test1.assertRowCount(0);        

        String[] sql1 = new String[100];
        
        for (int i = 0; i < 100; i++) {
            sql1[i] = "insert into g1 (e1, e2) values("+i+",'"+i+"')" ;
        }
        
        test1.executeBatch(sql1);
        test1.execute("select * from g1 ");
        test1.assertRowCount(100);
        test1.execute("select * from g2 ");
        test1.assertRowCount(0);        
        
        AbstractQueryTest test2 = new QueryExecution(ConnectionUtil.getSource("pm2")); //$NON-NLS-1$
        test2.execute("delete from g2"); //$NON-NLS-1$
        test2.execute("delete from g1");         //$NON-NLS-1$
        
        test2.execute("select * from g1 ");
        test2.assertRowCount(0);
        test2.execute("select * from g2 ");
        test2.assertRowCount(0);        
       
        String[] sql2 = new String[100];
        
        for (int i = 0; i < 100; i++) {
            sql2[i] = "insert into g2 (e1, e2) values("+i+",'"+i+"')" ;
        }
        
        test2.executeBatch(sql2);
        test2.execute("select * from g1 ");
        test2.assertRowCount(0);
        test2.execute("select * from g2 ");
        test2.assertRowCount(100);        
        
        System.out.println("TwoSource Setup Completed");

        

    }
	
	@Override
	public int getDataSourceCnt() {
		// TODO Auto-generated method stub
		return 2;
	}
 
}
