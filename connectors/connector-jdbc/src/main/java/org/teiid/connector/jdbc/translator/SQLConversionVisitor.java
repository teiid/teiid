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

import java.sql.Time;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.ILanguageObject;
import org.teiid.connector.language.ILimit;
import org.teiid.connector.language.ILiteral;
import org.teiid.connector.language.IParameter;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.language.IParameter.Direction;
import org.teiid.connector.language.ISetQuery.Operation;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;
import org.teiid.connector.visitor.util.SQLStringVisitor;


/**
 * This visitor takes an ICommand and does DBMS-specific conversion on it
 * to produce a SQL String.  This class is expected to be subclassed.
 * Specialized instances of this class can be gotten from a SQL Translator
 * {@link Translator#getTranslationVisitor(RuntimeMetadata) using this method}.
 */
public class SQLConversionVisitor extends SQLStringVisitor{

    private static DecimalFormat DECIMAL_FORMAT = 
        new DecimalFormat("#############################0.0#############################"); //$NON-NLS-1$    
    private static double SCIENTIC_LOW = Math.pow(10, -3);
    private static double SCIENTIC_HIGH = Math.pow(10, 7);
    
    private ExecutionContext context;
    private Translator translator;

    private boolean prepared;
    
    private List preparedValues = new ArrayList();
    
    private Set<ILanguageObject> recursionObjects = Collections.newSetFromMap(new IdentityHashMap<ILanguageObject, Boolean>());
    
    public SQLConversionVisitor(Translator translator) {
        this.translator = translator;
        this.prepared = translator.usePreparedStatements();
    }
    
    @Override
    public void append(ILanguageObject obj) {
    	if ((obj instanceof IFunction || obj instanceof ICommand || obj instanceof ILimit) && !recursionObjects.contains(obj)) {
	    	recursionObjects.add(obj);
	    	try {
	    		List<?> parts = null;
    	    	if (obj instanceof IFunction) {
    	    		IFunction function = (IFunction)obj;
    	    		Map<String, FunctionModifier> functionModifiers = translator.getFunctionModifiers();
    	    		if (functionModifiers != null) {
    	    			FunctionModifier modifier = functionModifiers.get(function.getName().toLowerCase());
    	    			if (modifier != null) {
    	    				parts = modifier.translate(function);
    	    			}
    	    		}
    	    	} else if (obj instanceof ICommand) {
    	    		parts = translator.translateCommand((ICommand)obj, context);
    	    	} else if (obj instanceof ILimit) {
    	    		parts = translator.translateLimit((ILimit)obj, context);
    	    	}
	    		if (parts != null) {
	    			this.appendParts(parts);
	    			return;
	    		}
	    	} finally {
	    		recursionObjects.remove(obj);
	    	}
    	} 
    	super.append(obj);
    }
    
	private void appendParts(List parts) {
		Iterator iter = parts.iterator();
		while(iter.hasNext()) {
		    Object part = iter.next();
		    if(part instanceof ILanguageObject) {
		        append((ILanguageObject)part);
		    } else {
		        buffer.append(part);
		    }
		}
	}

    /**
     * @param type
     * @param object
     * @param valuesbuffer
     */
    private void translateSQLType(Class type, Object obj, StringBuffer valuesbuffer) {
        if (obj == null) {
            valuesbuffer.append(NULL);
        } else {
            if(Number.class.isAssignableFrom(type)) {
                boolean useFormatting = false;
                
                if (Double.class.isAssignableFrom(type)){
                    double value = ((Double)obj).doubleValue();
                    useFormatting = (value <= SCIENTIC_LOW || value >= SCIENTIC_HIGH); 
                }
                else if (Float.class.isAssignableFrom(type)){
                    float value = ((Float)obj).floatValue();
                    useFormatting = (value <= SCIENTIC_LOW || value >= SCIENTIC_HIGH);
                }
                // The formatting is to avoid the so-called "scientic-notation"
                // where toString will use for numbers greater than 10p7 and
                // less than 10p-3, where database may not understand.
                if (useFormatting) {
                	synchronized (DECIMAL_FORMAT) {
                        valuesbuffer.append(DECIMAL_FORMAT.format(obj));
					}
                }
                else {
                    valuesbuffer.append(obj);
                }
            } else if(type.equals(TypeFacility.RUNTIME_TYPES.BOOLEAN)) {
                valuesbuffer.append(translator.translateLiteralBoolean((Boolean)obj));
            } else if(type.equals(TypeFacility.RUNTIME_TYPES.TIMESTAMP)) {
                valuesbuffer.append(translator.translateLiteralTimestamp((Timestamp)obj));
            } else if(type.equals(TypeFacility.RUNTIME_TYPES.TIME)) {
                valuesbuffer.append(translator.translateLiteralTime((Time)obj));
            } else if(type.equals(TypeFacility.RUNTIME_TYPES.DATE)) {
                valuesbuffer.append(translator.translateLiteralDate((java.sql.Date)obj));
            } else {
                // If obj is string, toSting() will not create a new String 
                // object, it returns it self, so new object creation. 
                valuesbuffer.append("'") //$NON-NLS-1$
                      .append(escapeString(obj.toString()))
                      .append("'"); //$NON-NLS-1$
            }
        }        
    }

    /**
     * @see org.teiid.connector.visitor.util.SQLStringVisitor#visit(org.teiid.connector.language.IProcedure)
     */
    public void visit(IProcedure obj) {
        this.prepared = true;
        /*
         * preparedValues is now a list of procedure params instead of just values
         */
        this.preparedValues = obj.getParameters();
        buffer.append(generateSqlForStoredProcedure(obj));
    }

    /**
     * @see org.teiid.connector.visitor.util.SQLStringVisitor#visit(org.teiid.connector.language.ILiteral)
     */
    public void visit(ILiteral obj) {
        if (this.prepared && obj.isBindValue()) {
            buffer.append(UNDEFINED_PARAM);
            preparedValues.add(obj);
        } else {
            translateSQLType(obj.getType(), obj.getValue(), buffer);
        }
    }

    /**
     * Set the per-command execution context on this visitor. 
     * @param context ExecutionContext
     * @since 4.3
     */
    public void setExecutionContext(ExecutionContext context) {
        this.context = context;
    }
    
    /**
     * Retrieve the per-command execution context for this visitor 
     * (intended for subclasses to use).
     * @return
     * @since 4.3
     */
    protected ExecutionContext getExecutionContext() {
        return this.context;
    }

    protected String getSourceComment(ICommand command) {
    	return this.translator.getSourceComment(this.context, command);
    }
    
    /**
     * This is a generic implementation. Subclass should override this method
     * if necessary.
     * @param exec The command for the stored procedure.
     * @return String to be executed by CallableStatement.
     */
    protected String generateSqlForStoredProcedure(IProcedure exec) {
        StringBuffer prepareCallBuffer = new StringBuffer();
        prepareCallBuffer.append("{ "); //$NON-NLS-1$

        List params = exec.getParameters();

        //check whether a "?" is needed if there are returns
        boolean needQuestionMark = false;
        Iterator iter = params.iterator();
        while(iter.hasNext()){
            IParameter param = (IParameter)iter.next();
            if(param.getDirection() == Direction.RETURN){
                needQuestionMark = true;
                break;
            }
        }
        
        prepareCallBuffer.append(getSourceComment(exec));
        
        if(needQuestionMark){
            prepareCallBuffer.append("?="); //$NON-NLS-1$
        }

        prepareCallBuffer.append(" call ");//$NON-NLS-1$
        prepareCallBuffer.append(exec.getMetadataObject() != null ? getName(exec.getMetadataObject()) : exec.getProcedureName());
        prepareCallBuffer.append("("); //$NON-NLS-1$

        int numberOfParameters = 0;
        iter = params.iterator();
        while(iter.hasNext()){
            IParameter param = (IParameter)iter.next();
            if(param.getDirection() == Direction.IN || param.getDirection() == Direction.OUT || param.getDirection() == Direction.INOUT){
                if(numberOfParameters > 0){
                    prepareCallBuffer.append(","); //$NON-NLS-1$
                }
                prepareCallBuffer.append("?"); //$NON-NLS-1$
                numberOfParameters++;
            }
        }
        prepareCallBuffer.append(")"); //$NON-NLS-1$
        prepareCallBuffer.append("}"); //$NON-NLS-1$
        return prepareCallBuffer.toString();
    }
    
    /** 
     * @return the preparedValues
     */
    List getPreparedValues() {
        return this.preparedValues;
    }
    
    public boolean isPrepared() {
		return prepared;
	}
    
    public void setPrepared(boolean prepared) {
		this.prepared = prepared;
	}
    
    @Override
    protected boolean useAsInGroupAlias() {
    	return this.translator.useAsInGroupAlias();
    }
        
    @Override
    protected boolean useParensForSetQueries() {
    	return translator.useParensForSetQueries();
    }
    	
	@Override
	protected String replaceElementName(String group, String element) {
		return translator.replaceElementName(group, element);
	}
	
	@Override
	protected void appendSetOperation(Operation operation) {
		buffer.append(translator.getSetOperationString(operation));
	}
    
	@Override
    protected boolean useParensForJoins() {
    	return translator.useParensForJoins();
    }
}
