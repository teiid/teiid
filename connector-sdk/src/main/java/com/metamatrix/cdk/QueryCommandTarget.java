/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
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

package com.metamatrix.cdk;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.commandshell.ScriptCommandTarget;

/**
 * Base CommandTarget class for processing SQL queries via the command line.
 */
abstract public class QueryCommandTarget extends ScriptCommandTarget {

	abstract protected String execute(String query);
    protected boolean usePreparedStatement = false;
    protected boolean prepareStmt = false;
    protected Map preparedStmts = new HashMap();
    protected String currentPreparedStmtName = null;
    
    protected String readSql(String commandName, String[] args) {
        StringBuffer query = new StringBuffer();
        StringBuffer firstLine = new StringBuffer();
        firstLine.append(commandName);
        firstLine.append(" "); //$NON-NLS-1$

        for (int i = 0; i < args.length; i++) {
            firstLine.append(args[i]);
            firstLine.append(" "); //$NON-NLS-1$
        }

        String line = firstLine.toString();

        boolean cont = true;
        while (cont) {
            try {
                if (line != null && line.trim().endsWith(";")) { //$NON-NLS-1$
                    cont = false;
                    line = line.trim();
                    line = line.substring(0, line.length() - 1);
                }
                query.append(" "); //$NON-NLS-1$
                query.append(line);
                if (cont) {
                    line = shell.getNextCommandLine();
                }
            } catch (IOException e) {
                throw new MetaMatrixRuntimeException(e);
            }
        }
        return query.toString();            
    }
    
    public void setUsePreparedStatement(boolean usePreparedStatement) {
        this.usePreparedStatement = usePreparedStatement;
    }
    
    protected String executeSql(String commandName, String[] args) {
        if (!this.usePreparedStatement) {
            prepareStmt = false;
        }
        
        if (prepareStmt) {
            preparedStmts.put(currentPreparedStmtName, readSql(commandName, args));
            return ""; //$NON-NLS-1$
        }
        return execute(readSql(commandName, args));
    }
    
	public String select(String[] args) { 
		return executeSql("select", args); //$NON-NLS-1$
	}

	public String insert(String[] args) {
		return executeSql("insert", args); //$NON-NLS-1$
	}

	public String update(String[] args) {
		return executeSql("update", args); //$NON-NLS-1$
	}

	public String delete(String[] args) {
		return executeSql("delete", args); //$NON-NLS-1$
	}
    
    public String exec(String[] args) {
        return executeSql("exec", args); //$NON-NLS-1$
    }    
}
