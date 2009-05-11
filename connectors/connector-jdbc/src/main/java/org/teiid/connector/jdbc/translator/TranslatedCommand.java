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
package org.teiid.connector.jdbc.translator;

import java.util.List;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.ILiteral;
import org.teiid.connector.visitor.framework.DelegatingHierarchyVisitor;
import org.teiid.connector.visitor.util.CollectorVisitor;


/**
 * This is a utility class used to translate an ICommand using a SQLConversionVisitor.
 * The SQLConversionVisitor should not be invoked directly; this object will use it to
 * translate the ICommand.
 */
public class TranslatedCommand {

    private String sql;
    private boolean prepared;
    private List preparedValues;
    
    private SQLConversionVisitor sqlConversionVisitor;
    private ReplacementVisitor functionVisitor;
    private ExecutionContext context;
    private Translator sqlTranslator;
    
    /**
     * Constructor, takes a SQLConversionVisitor subclass 
     * @param visitor a SQLConversionVisitor subclass 
     */
    public TranslatedCommand(ExecutionContext context, Translator sqlTranslator){
    	this.context = context;
    	this.sqlTranslator = sqlTranslator;
        this.sqlConversionVisitor = sqlTranslator.getSQLConversionVisitor();
        this.sqlConversionVisitor.setExecutionContext(context);
        this.functionVisitor = new ReplacementVisitor(context, sqlTranslator);
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
        this.preparedValues = this.sqlConversionVisitor.getPreparedValues();
        this.prepared = this.sqlConversionVisitor.isPrepared();
    }
	
	private String getSQL(ICommand command) throws ConnectorException {
        command = sqlTranslator.modifyCommand(command, context);
		command.acceptVisitor(new DelegatingHierarchyVisitor(null, this.functionVisitor));
        
        if (sqlTranslator.usePreparedStatements() || hasBindValue(command)) {
            this.sqlConversionVisitor.setPrepared(true);
            
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
        for (ILiteral l : CollectorVisitor.collectObjects(ILiteral.class, command)) {
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
