/*
 * Copyright � 2000-2006 MetaMatrix, Inc.
 * All rights reserved.
 */
package com.metamatrix.installer.anttask.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.JDBCTask;

import com.metamatrix.core.util.ArgCheck;




/** 
 * @since 5.0
 */
// public class DBTypeTask extends  SQLExec{
public class DBTypeTask extends  JDBCTask {

	private String dbTypeProperty;
    

    public void setDatabaseTypeProperty(String property) {
		this.dbTypeProperty = property;

    }
    public void execute() throws BuildException {
        ArgCheck.isNotNull(this.dbTypeProperty, "DatabaseTypeProperty was not set.");
        Connection conn = null;
        try {
	        conn = this.getConnection();
	        String rdbmstype = conn.getMetaData().getDatabaseProductName().toLowerCase();

	        	//this.getRdbms();
	        this.getProject().setNewProperty(this.dbTypeProperty, rdbmstype);
	        this.getProject().log("RDBMS type is " + rdbmstype, Project.MSG_WARN);
	       	
        } catch (SQLException e) {
     	   String msg = "Unable to get driver product information, error " + e.getMessage();
    	   this.getProject().log(msg, Project.MSG_ERR);
    	   throw new BuildException(e); //$NON-NLS-1$

		} finally {
        	if (conn != null) {
        		try {
        			conn.close();
        		} catch (Exception e) {
        			
        		}
        	}
        }
    }
}
