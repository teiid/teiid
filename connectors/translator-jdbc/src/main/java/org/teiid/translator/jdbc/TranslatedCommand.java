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

import java.util.List;

import org.teiid.language.Command;
import org.teiid.language.Literal;
import org.teiid.language.visitor.CollectorVisitor;
import org.teiid.translator.ConnectorException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TypeFacility;


/**
 * This is a utility class used to translate an ICommand using a SQLConversionVisitor.
 * The SQLConversionVisitor should not be invoked directly; this object will use it to
 * translate the ICommand.
 */
public class TranslatedCommand {

    private String sql;
    private boolean prepared;
    private List preparedValues;
    
    private JDBCExecutionFactory executionFactory;
    private ExecutionContext context;
    
    /**
     * Constructor, takes a SQLConversionVisitor subclass 
     * @param visitor a SQLConversionVisitor subclass 
     */
    public TranslatedCommand(ExecutionContext context, JDBCExecutionFactory executionFactory){
    	this.executionFactory = executionFactory;
    	this.context = context;
    }
    
    /**
     * The method to cause this object to do it's thing.  This method should
     * be called right after the constructor; afterward, all of the getter methods
     * can be called to retrieve results. 
     * @param command ICommand to be translated
     * @throws ConnectorException 
     */
    public void translateCommand(Command command) throws ConnectorException {
    	SQLConversionVisitor sqlConversionVisitor = executionFactory.getSQLConversionVisitor();
        sqlConversionVisitor.setExecutionContext(context);
        if (executionFactory.usePreparedStatements() || hasBindValue(command)) {
        	sqlConversionVisitor.setPrepared(true);
        }
        
		sqlConversionVisitor.append(command);
		this.sql = sqlConversionVisitor.toString();
        this.preparedValues = sqlConversionVisitor.getPreparedValues();
        this.prepared = sqlConversionVisitor.isPrepared();
    }
	
    /**
     * Simple check to see if any values in the command should be replaced with bind values
     *  
     * @param command
     * @return
     */
    private boolean hasBindValue(Command command) {
        for (Literal l : CollectorVisitor.collectObjects(Literal.class, command)) {
            if (l.isBindValue() || isBindEligible(l)) {
                return true;
            }
        }
        return false;
    }

    /** 
     * @param l
     * @return
     */
    static boolean isBindEligible(Literal l) {
		return TypeFacility.RUNTIME_TYPES.XML.equals(l.getType())
				|| TypeFacility.RUNTIME_TYPES.CLOB.equals(l.getType())
				|| TypeFacility.RUNTIME_TYPES.BLOB.equals(l.getType())
				|| TypeFacility.RUNTIME_TYPES.OBJECT.equals(l.getType());
	}
    
    /**
     * Return List of values to set on a prepared statement, if 
     * necessary (see {@link #getStatementType})
     * @return List of values to be set on a prepared statement
     */
    public List getPreparedValues() {
        return preparedValues;
    }
    
    /**
     * Get String SQL of translated command; but use 
     * {@link #nextLargeSetQuerySQL} if this command is 
     * a {@link #isLargeSetQuery large set query}
     * @return SQL of translated command, or null if the
     * command is a {@link #isLargeSetQuery large set query}
     */
    public String getSql() {
        return sql;
    }

    /**
     * Return the statement type, one of {@link #STMT_TYPE_STATEMENT},
     * {@link #STMT_TYPE_PREPARED_STATEMENT}, or
     * {@link #STMT_TYPE_CALLABLE_STATEMENT}
     * @return statement type of translated command
     */
    public boolean isPrepared() {
        return prepared;
    }
    
    @Override
    public String toString() {
    	StringBuffer sb = new StringBuffer();
    	if (prepared) {
    		sb.append("Prepared Values: ").append(preparedValues).append(" "); //$NON-NLS-1$ //$NON-NLS-2$
    	}
    	sb.append("SQL: ").append(sql); //$NON-NLS-1$
    	return sb.toString();
    }

}
