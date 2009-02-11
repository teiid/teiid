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

/*
 */
package com.metamatrix.connector.jdbc.extension;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.TypeFacility;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.jdbc.util.FunctionReplacementVisitor;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.language.ILiteral;
import com.metamatrix.connector.visitor.util.CollectorVisitor;

/**
 * This is a utility class used to translate an ICommand using a SQLConversionVisitor.
 * The SQLConversionVisitor should not be invoked directly; this object will use it to
 * translate the ICommand.
 */
public class TranslatedCommand {

    /** the translated command is a query */
    public static final int EXEC_TYPE_QUERY = 0;

    /** the translated command is an insert, update, or delete */
    public static final int EXEC_TYPE_UPDATE = 1;

    /** the translated command is an execute-type command */
    public static final int EXEC_TYPE_EXECUTE = 2;
    
    /** execution of translated command requires a jdbc Statement */
    public static final int STMT_TYPE_STATEMENT = 0;

    /** execution of translated command requires a jdbc Prepared Statement */
    public static final int STMT_TYPE_PREPARED_STATEMENT = 1;

    /** execution of translated command requires a jdbc Callable Statement */
    public static final int STMT_TYPE_CALLABLE_STATEMENT = 2;
    
    private String sql;
    private int executionType;
    private int statementType;
    private List preparedValues;
    private List preparedTypes;
    
    private SQLConversionVisitor sqlConversionVisitor;
    private FunctionReplacementVisitor functionVisitor;
    private ExecutionContext context;
    private SQLTranslator sqlTranslator;
    
    /**
     * Constructor, takes a SQLConversionVisitor subclass 
     * @param visitor a SQLConversionVisitor subclass 
     */
    public TranslatedCommand(ExecutionContext context, SQLTranslator sqlTranslator){
    	this.context = context;
    	this.sqlTranslator = sqlTranslator;
    	
    	Map modifiers = sqlTranslator.getFunctionModifiers();
        this.sqlConversionVisitor = sqlTranslator.getTranslationVisitor();
        sqlConversionVisitor.setFunctionModifiers(modifiers);
        sqlConversionVisitor.setExecutionContext(context);
        this.functionVisitor = new FunctionReplacementVisitor(modifiers);
    }
    
    public TranslatedCommand(ExecutionContext context, SQLTranslator sqlTranslator, SQLConversionVisitor sqlConversionVisitor, FunctionReplacementVisitor functionVisitor) {
    	this.context = context;
    	this.sqlTranslator = sqlTranslator;
    	this.sqlConversionVisitor = sqlConversionVisitor;
    	this.functionVisitor = functionVisitor;
    }
    
    /**
     * The method to cause this object to do it's thing.  This method should
     * be called right after the constructor; afterward, all of the getter methods
     * can be called to retrieve results. 
     * @param command ICommand to be translated
     * @throws ConnectorException 
     */
    public void translateCommand(ICommand command) throws ConnectorException {
        this.sql = getSQL(command);
        this.statementType = this.sqlConversionVisitor.getStmtType();
        this.executionType = this.sqlConversionVisitor.getExecType();
        this.preparedValues = this.sqlConversionVisitor.getPreparedValues();
        this.preparedTypes = this.sqlConversionVisitor.getPreparedTypes();
    }
	
	private String getSQL(ICommand command) throws ConnectorException {
        command = sqlTranslator.modifyCommand(command, context);
		command.acceptVisitor(functionVisitor);
        
        if (this.sqlConversionVisitor.getStmtType() == STMT_TYPE_PREPARED_STATEMENT || hasBindValue(command)) {
            this.sqlConversionVisitor.setStmtType(STMT_TYPE_PREPARED_STATEMENT);
            
            command.acceptVisitor(new BindValueVisitor());
        }
        
		this.sqlConversionVisitor.append(command);
		return this.sqlConversionVisitor.toString();
	}

    /**
     * Simple check to see if any values in the command should be replaced with bind values
     *  
     * @param command
     * @return
     */
    private boolean hasBindValue(ICommand command) {
        Collection literals = CollectorVisitor.collectObjects(ILiteral.class, command);
        
        for (Iterator i = literals.iterator(); i.hasNext();) {
            ILiteral l = (ILiteral)i.next();
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
    static boolean isBindEligible(ILiteral l) {
        if (TypeFacility.RUNTIME_TYPES.CLOB.equals(l.getType())
                        || TypeFacility.RUNTIME_TYPES.BLOB.equals(l.getType()) 
                        || TypeFacility.RUNTIME_TYPES.OBJECT.equals(l.getType())) {
            return true;
        }
        return false;
    }
    
    /**
     * Return the execution type, one of {@link #EXEC_TYPE_QUERY},
     * {@link #EXEC_TYPE_UPDATE}, or
     * {@link #EXEC_TYPE_EXECUTE}
     * @return execution type of translated command
     */
    public int getExecutionType() {
        return executionType;
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
     * Return List of types to set on a prepared statement, if 
     * necessary (see {@link #getStatementType})
     * @return List of types to be set on a prepared statement
     */
    public List getPreparedTypes() {
        return preparedTypes;
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
    public int getStatementType() {
        return statementType;
    }

}
