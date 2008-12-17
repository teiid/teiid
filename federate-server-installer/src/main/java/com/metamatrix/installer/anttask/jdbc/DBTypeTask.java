/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
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
