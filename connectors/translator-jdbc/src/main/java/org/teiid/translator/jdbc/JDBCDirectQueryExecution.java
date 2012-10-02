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

/*
 */

package org.teiid.translator.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.teiid.language.Argument;
import org.teiid.language.Command;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;


public class JDBCDirectQueryExecution extends JDBCQueryExecution implements ProcedureExecution {

    protected int columnCount;
    private List<Argument> arguments;
    protected int updateCount = -1;

    public JDBCDirectQueryExecution(List<Argument> arguments, Command command, Connection connection, ExecutionContext context, JDBCExecutionFactory env) {
        super(command, connection, context, env);
        this.arguments = arguments;
    }
    
    @Override
    public void execute() throws TranslatorException {
    	String sourceSQL = (String) this.arguments.get(0).getArgumentValue().getValue();
    	List<Argument> parameters = this.arguments.subList(1, this.arguments.size());
    			
        LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Source sql", sourceSQL); //$NON-NLS-1$
        int paramCount = parameters.size();
        
        try {
        	Statement stmt;
        	boolean hasResults = false;
    	
        	if(paramCount > 0) {
            	PreparedStatement pstatement = getPreparedStatement(sourceSQL);
            	for (int i = 0; i < paramCount; i++) {
            		Argument arg = parameters.get(i);
            		//TODO: if ParameterMetadata is supported we could use that type
            		this.executionFactory.bindValue(pstatement, arg.getArgumentValue(), arg.getArgumentValue().getType(), i);
            	}
                stmt = pstatement;
                hasResults = pstatement.execute();
            }
        	else {
        		//TODO: when array support becomes more robust calling like "exec native('sql', ARRAY[]) could still be prepared
        		stmt = getStatement();
        		hasResults = stmt.execute(sourceSQL);
        	}
    		
        	if (hasResults) {
    			this.results = stmt.getResultSet();
    			this.columnCount = this.results.getMetaData().getColumnCount();
    		}
    		else {
    			this.updateCount = stmt.getUpdateCount();
    		}
            addStatementWarnings();
        } catch (SQLException e) {
             throw new JDBCExecutionException(JDBCPlugin.Event.TEIID11008, e, sourceSQL);
        }
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        try {
        	ArrayList<Object[]> row = new ArrayList<Object[]>(1);
        	
        	if (this.results != null) {
	            if (this.results.next()) {
	                // New row for result set
	                List<Object> vals = new ArrayList<Object>(this.columnCount);
	
	                for (int i = 0; i < this.columnCount; i++) {
	                    // Convert from 0-based to 1-based
	                    Object value = this.executionFactory.retrieveValue(this.results, i+1, TypeFacility.RUNTIME_TYPES.OBJECT);
	                    vals.add(value); 
	                }
	                row.add(vals.toArray(new Object[vals.size()]));
	                return row;
	            } 
        	}
        	else if (this.updateCount != -1) {
        		List<Object> vals = new ArrayList<Object>(1);
        		vals.add(new Integer(this.updateCount));
        		this.updateCount = -1;
                row.add(vals.toArray(new Object[vals.size()]));
                return row;
        	}
        } catch (SQLException e) {
            throw new TranslatorException(e,JDBCPlugin.Util.getString("JDBCTranslator.Unexpected_exception_translating_results___8", e.getMessage())); //$NON-NLS-1$
        }
        return null;
    }
    
	@Override
	public List<?> getOutputParameterValues() throws TranslatorException {
		return null;  //could support as an array of output values via given that the native procedure returns an array value
	}
}
