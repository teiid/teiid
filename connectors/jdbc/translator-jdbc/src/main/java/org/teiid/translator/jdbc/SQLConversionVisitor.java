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

import static org.teiid.language.SQLConstants.Reserved.*;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.teiid.core.types.BinaryType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.*;
import org.teiid.language.Argument.Direction;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.SetQuery.Operation;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.Procedure;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TypeFacility;


/**
 * This visitor takes an ICommand and does DBMS-specific conversion on it
 * to produce a SQL String.  This class is expected to be subclassed.
 */
public class SQLConversionVisitor extends SQLStringVisitor implements SQLStringVisitor.Substitutor {
	public static final String TEIID_NON_PREPARED = AbstractMetadataRecord.RELATIONAL_URI + "non-prepared"; //$NON-NLS-1$

    private static DecimalFormat DECIMAL_FORMAT = 
        new DecimalFormat("#############################0.0#############################", DecimalFormatSymbols.getInstance(Locale.US)); //$NON-NLS-1$    
    private static double SCIENTIFIC_LOW = Math.pow(10, -3);
    private static double SCIENTIFIC_HIGH = Math.pow(10, 7);
    
    private ExecutionContext context;
    private JDBCExecutionFactory executionFactory;

    private boolean prepared;
    private boolean usingBinding;
    
    private List preparedValues = new ArrayList();
    
    private Set<LanguageObject> recursionObjects = Collections.newSetFromMap(new IdentityHashMap<LanguageObject, Boolean>());
    private Map<LanguageObject, Object> translations = new IdentityHashMap<LanguageObject, Object>(); 
    
    private boolean replaceWithBinding = false;
    
    public SQLConversionVisitor(JDBCExecutionFactory ef) {
        this.executionFactory = ef;
        this.prepared = executionFactory.usePreparedStatements();
    }
    
    @Override
    public void append(LanguageObject obj) {
    	if (shortNameOnly && obj instanceof ColumnReference) {
    		super.append(obj);
    		return;
    	}
        boolean replacementMode = replaceWithBinding;
        if (obj instanceof Command || obj instanceof Function) {
    	    /*
    	     * In general it is not appropriate to use bind values within a function
    	     * unless the particulars of the function parameters are know.  
    	     * As needed, other visitors or modifiers can set the literals used within
    	     * a particular function as bind variables.  
    	     */
        	this.replaceWithBinding = false;
        }
    	List<?> parts = null;
    	if (!recursionObjects.contains(obj)) {
    		Object trans = this.translations.get(obj);
    		if (trans instanceof List<?>) {
    			parts = (List<?>)trans;
    		} else if (trans instanceof LanguageObject) {
    			obj = (LanguageObject)trans;
    		} else {
    			parts = executionFactory.translate(obj, context);
    			if (parts != null) {
    				this.translations.put(obj, parts);
    			} else {
    				this.translations.put(obj, obj);
    			}
    		}
    	}
		if (parts != null) {
			recursionObjects.add(obj);
			for (Object part : parts) {
			    if(part instanceof LanguageObject) {
			        append((LanguageObject)part);
			    } else {
			        buffer.append(part);
			    }
			}
			recursionObjects.remove(obj);
		} else {
			super.append(obj);
		}
        this.replaceWithBinding = replacementMode;
    }
    
    /**
     * @param type
     * @param object
     * @param valuesbuffer
     */
    protected void translateSQLType(Class<?> type, Object obj, StringBuilder valuesbuffer) {
        if (obj == null) {
            valuesbuffer.append(Reserved.NULL);
        } else {
            if(Number.class.isAssignableFrom(type)) {
                boolean useFormatting = false;
            	if (!executionFactory.useScientificNotation()) {
	                if (Double.class.isAssignableFrom(type)){
	                    double value = Math.abs(((Double)obj).doubleValue());
	                    useFormatting = (value <= SCIENTIFIC_LOW || value >= SCIENTIFIC_HIGH); 
	                }
	                else if (Float.class.isAssignableFrom(type)){
	                    float value = Math.abs(((Float)obj).floatValue());
	                    useFormatting = (value <= SCIENTIFIC_LOW || value >= SCIENTIFIC_HIGH);
	                } else if (BigDecimal.class.isAssignableFrom(type)) {
	                	valuesbuffer.append(((BigDecimal)obj).toPlainString());
	                	return;
	                }
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
                valuesbuffer.append(executionFactory.translateLiteralBoolean((Boolean)obj));
            } else if(type.equals(TypeFacility.RUNTIME_TYPES.TIMESTAMP)) {
                valuesbuffer.append(executionFactory.translateLiteralTimestamp((Timestamp)obj));
            } else if(type.equals(TypeFacility.RUNTIME_TYPES.TIME)) {
            	if (!executionFactory.hasTimeType()) {
            		valuesbuffer.append(executionFactory.translateLiteralTimestamp(new Timestamp(((Time)obj).getTime())));
            	} else {
            		valuesbuffer.append(executionFactory.translateLiteralTime((Time)obj));
            	}
            } else if(type.equals(TypeFacility.RUNTIME_TYPES.DATE)) {
                valuesbuffer.append(executionFactory.translateLiteralDate((java.sql.Date)obj));
            } else if (type.equals(DataTypeManager.DefaultDataClasses.VARBINARY)) {
            	valuesbuffer.append(executionFactory.translateLiteralBinaryType((BinaryType)obj));
            } else {
                // If obj is string, toSting() will not create a new String 
                // object, it returns it self, so new object creation.
            	String val = obj.toString();
            	if (useUnicodePrefix()) {
	            	for (int i = 0; i < val.length(); i++) {
	    				if (val.charAt(i) > 127) {
	    					buffer.append("N"); //$NON-NLS-1$
	    					break;
	    				}
	    			}
            	}
                valuesbuffer.append(Tokens.QUOTE)
                      .append(escapeString(val, Tokens.QUOTE))
                      .append(Tokens.QUOTE);
            }
        }        
    }
    
    protected boolean useUnicodePrefix() {
    	return this.executionFactory.useUnicodePrefix();
    }

    /**
     * @see org.teiid.language.visitor.SQLStringVisitor#visit(org.teiid.language.Call)
     */
    public void visit(Call obj) {
    	usingBinding = true;
    	Procedure p = obj.getMetadataObject();
    	if (p != null) {
	    	String nativeQuery = p.getProperty(TEIID_NATIVE_QUERY, false);
	    	if (nativeQuery != null) {
	    		this.prepared = !Boolean.valueOf(p.getProperty(TEIID_NON_PREPARED, false));
	    		if (this.prepared) {
	    			this.preparedValues = new ArrayList<Object>();
	    		}
	    		parseNativeQueryParts(nativeQuery, obj.getArguments(), buffer, this);
	    		return;
	    	}
    	}
    	if (obj.isTableReference()) {
    		usingBinding = false;
    		super.visit(obj);
    		return;
    	}
		this.prepared = true;
		/*
         * preparedValues is now a list of procedure params instead of just values
         */
        this.preparedValues = obj.getArguments();
        buffer.append(generateSqlForStoredProcedure(obj));
    }
    
    public void visit(Function obj) {
    	FunctionMethod f = obj.getMetadataObject();
    	if (f != null) {
	    	String nativeQuery = f.getProperty(TEIID_NATIVE_QUERY, false);
	    	if (nativeQuery != null) {
	    		List<Argument> args = new ArrayList<Argument>(obj.getParameters().size());
	    		for (Expression p : obj.getParameters()) {
	    			args.add(new Argument(Direction.IN, p, p.getType(), null));
	    		}
	    		parseNativeQueryParts(nativeQuery, args, buffer, this);
	    		return;
	    	}
    	}
    	super.visit(obj);
    }

	@Override
	public void visit(Parameter obj) {
        buffer.append(UNDEFINED_PARAM);
        preparedValues.add(obj);
        usingBinding = true;
	}
    
    /**
     * @see org.teiid.language.visitor.SQLStringVisitor#visit(org.teiid.language.Literal)
     */
    public void visit(Literal obj) {
        if (this.prepared && ((replaceWithBinding && obj.isBindEligible()) || TranslatedCommand.isBindEligible(obj))) {
            buffer.append(UNDEFINED_PARAM);
            preparedValues.add(obj);
            usingBinding = true;
        } else {
            translateSQLType(obj.getType(), obj.getValue(), buffer);
        }
    }
    
    @Override
    public void visit(In obj) {
        replaceWithBinding = true;
        super.visit(obj);
    }

    @Override
    public void visit(Like obj) {
        replaceWithBinding = true;
        super.visit(obj);
    }

    @Override
    public void visit(Comparison obj) {
        replaceWithBinding = true;
        super.visit(obj);
    }

    @Override
    public void visit(ExpressionValueSource obj) {
        replaceWithBinding = true;
        super.visit(obj);
    }
    
    @Override
    public void visit(SetClause obj) {
        replaceWithBinding = true;
        super.visit(obj);
    }
    
    @Override
    public void visit(DerivedColumn obj) {
    	replaceWithBinding = false;
    	super.visit(obj);
    }
    
    @Override
    public void visit(SearchedCase obj) {
    	replaceWithBinding = false;
    	super.visit(obj);
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

    protected String getSourceComment(Command command) {
    	return this.executionFactory.getSourceComment(this.context, command);
    }
    
    /**
     * This is a generic implementation. Subclass should override this method
     * if necessary.
     * @param exec The command for the stored procedure.
     * @return String to be executed by CallableStatement.
     */
    protected String generateSqlForStoredProcedure(Call exec) {
        StringBuffer prepareCallBuffer = new StringBuffer();
        prepareCallBuffer.append(getSourceComment(exec));
        prepareCallBuffer.append("{"); //$NON-NLS-1$

        List<Argument> params = exec.getArguments();

        //check whether a "?" is needed if there are returns
        boolean needQuestionMark = exec.getReturnType() != null;
        
        if(needQuestionMark){
            prepareCallBuffer.append("?= "); //$NON-NLS-1$
        }

        prepareCallBuffer.append("call ");//$NON-NLS-1$
        prepareCallBuffer.append(exec.getMetadataObject() != null ? getName(exec.getMetadataObject()) : exec.getProcedureName());
        prepareCallBuffer.append("("); //$NON-NLS-1$

        int numberOfParameters = 0;
        for (Argument param : params) {
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
    
    public boolean isUsingBinding() {
		return usingBinding;
	}
    
    @Override
    protected boolean useAsInGroupAlias() {
    	return this.executionFactory.useAsInGroupAlias();
    }
        
    @Override
    protected boolean useParensForSetQueries() {
    	return executionFactory.useParensForSetQueries();
    }
    	
	@Override
	protected String replaceElementName(String group, String element) {
		return executionFactory.replaceElementName(group, element);
	}
	
	@Override
	protected void appendSetOperation(Operation operation) {
		buffer.append(executionFactory.getSetOperationString(operation));
	}
    
	@Override
    protected boolean useParensForJoins() {
    	return executionFactory.useParensForJoins();
    }
	
	protected boolean useSelectLimit() {
		return executionFactory.useSelectLimit();
	}
	
	@Override
	protected String getLikeRegexString() {
		return executionFactory.getLikeRegexString();
	}
	
	@Override
	protected void appendBaseName(NamedTable obj) {
		if (obj.getMetadataObject() != null) {
			String nativeQuery = obj.getMetadataObject().getProperty(TEIID_NATIVE_QUERY, false);
	    	if (nativeQuery != null) {
	    		if (obj.getCorrelationName() == null) {
	    			throw new IllegalArgumentException(JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11020, obj.getName()));
	    		}
	    		buffer.append(Tokens.LPAREN).append(nativeQuery).append(Tokens.RPAREN);
	    		if (obj.getCorrelationName() == null) {
	                buffer.append(Tokens.SPACE);
	    			if (useAsInGroupAlias()){
	                    buffer.append(AS).append(Tokens.SPACE);
	                }
	    		}
	    		return;
	    	}
		}
		super.appendBaseName(obj);
	}

	@Override
	public void substitute(Argument arg, StringBuilder builder, int index) {
		if (this.prepared && arg.getExpression() instanceof Literal) {
			buffer.append('?');
			this.preparedValues.add(arg);
		} else {
			visit(arg);
		}
	}
	
	@Override
	public void visit(GroupBy obj) {
		if (obj.isRollup() && executionFactory.useWithRollup()) {
			obj.setRollup(false);
			super.visit(obj);
			obj.setRollup(true);
			buffer.append(" WITH ROLLUP"); //$NON-NLS-1$
			return;
		}
		super.visit(obj);
	}
	
	@Override
	protected void appendLateralKeyword() {
		buffer.append(this.executionFactory.getLateralKeyword());
	}
	
}
