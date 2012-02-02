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

import java.sql.Time;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.language.*;
import org.teiid.language.Argument.Direction;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.SetQuery.Operation;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Procedure;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TypeFacility;


/**
 * This visitor takes an ICommand and does DBMS-specific conversion on it
 * to produce a SQL String.  This class is expected to be subclassed.
 */
public class SQLConversionVisitor extends SQLStringVisitor{
	public static final String TEIID_NATIVE_QUERY = AbstractMetadataRecord.RELATIONAL_URI + "native-query"; //$NON-NLS-1$
	public static final String TEIID_NON_PREPARED = AbstractMetadataRecord.RELATIONAL_URI + "non-prepared"; //$NON-NLS-1$

    private static DecimalFormat DECIMAL_FORMAT = 
        new DecimalFormat("#############################0.0#############################"); //$NON-NLS-1$    
    private static double SCIENTIC_LOW = Math.pow(10, -3);
    private static double SCIENTIC_HIGH = Math.pow(10, 7);
    
    private ExecutionContext context;
    private JDBCExecutionFactory executionFactory;

    private boolean prepared;
    
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
    private void translateSQLType(Class type, Object obj, StringBuilder valuesbuffer) {
        if (obj == null) {
            valuesbuffer.append(Reserved.NULL);
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
                valuesbuffer.append(executionFactory.translateLiteralBoolean((Boolean)obj));
            } else if(type.equals(TypeFacility.RUNTIME_TYPES.TIMESTAMP)) {
                valuesbuffer.append(executionFactory.translateLiteralTimestamp((Timestamp)obj));
            } else if(type.equals(TypeFacility.RUNTIME_TYPES.TIME)) {
                valuesbuffer.append(executionFactory.translateLiteralTime((Time)obj));
            } else if(type.equals(TypeFacility.RUNTIME_TYPES.DATE)) {
                valuesbuffer.append(executionFactory.translateLiteralDate((java.sql.Date)obj));
            } else {
                // If obj is string, toSting() will not create a new String 
                // object, it returns it self, so new object creation. 
                valuesbuffer.append(Tokens.QUOTE)
                      .append(escapeString(obj.toString(), Tokens.QUOTE))
                      .append(Tokens.QUOTE);
            }
        }        
    }

    /**
     * @see org.teiid.language.visitor.SQLStringVisitor#visit(org.teiid.language.Call)
     */
    public void visit(Call obj) {
    	Procedure p = obj.getMetadataObject();
    	if (p != null) {
	    	String nativeQuery = p.getProperty(TEIID_NATIVE_QUERY, false);
	    	if (nativeQuery != null) {
	    		List<Object> parts = parseNativeQueryParts(nativeQuery);
	    		this.prepared = !Boolean.valueOf(p.getProperty(TEIID_NON_PREPARED, false));
	    		if (this.prepared) {
	    			this.preparedValues = new ArrayList<Object>();
	    		}
	    		for (Object o : parts) {
	    			if (o instanceof String) {
	    				buffer.append(o);
	    			} else {
	    				Integer i = (Integer)o;
	    				if (i < 0 || i >= obj.getArguments().size()) {
	    					throw new IllegalArgumentException(JDBCPlugin.Util.getString("SQLConversionVisitor.invalid_parameter", i+1, obj.getArguments().size())); //$NON-NLS-1$
	    				}
	    				if (obj.getArguments().get(i).getDirection() != Direction.IN) {
	    					throw new IllegalArgumentException(JDBCPlugin.Util.getString("SQLConversionVisitor.not_in_parameter", i+1)); //$NON-NLS-1$
	    				}
	    				buffer.append('?');
	    				if (this.prepared) {
	    					this.preparedValues = obj.getArguments();
	    				}
	    			}
	    		}
	    		return;
	    	}
    	}
		this.prepared = true;
		/*
         * preparedValues is now a list of procedure params instead of just values
         */
        this.preparedValues = obj.getArguments();
        buffer.append(generateSqlForStoredProcedure(obj));
    }

	private List<Object> parseNativeQueryParts(String nativeQuery) {
		Pattern pattern = Pattern.compile("\\$+\\d+"); //$NON-NLS-1$
		List<Object> parts = new LinkedList<Object>();
		Matcher m = pattern.matcher(nativeQuery);
		for (int i = 0; i < nativeQuery.length(); i++) {
			if (!m.find(i)) {
				parts.add(nativeQuery.substring(i));
				break;
			}
			if (m.start() != i) {
				parts.add(nativeQuery.substring(i, m.start()));
			}
			String match = m.group();
			int end = match.lastIndexOf('$');
			if ((end&0x1) == 1) {
				//escaped
				parts.add(match.substring((end+1)/2)); 
			} else {
				if (end != 0) {
					parts.add(match.substring(0, end/2));
				}
				parts.add(Integer.parseInt(match.substring(end + 1))-1);
			}
			i = m.end();
		}
		return parts;
	}
    
    /**
     * @see org.teiid.language.visitor.SQLStringVisitor#visit(org.teiid.language.Literal)
     */
    public void visit(Literal obj) {
        if (this.prepared && ((replaceWithBinding && obj.isBindEligible()) || TranslatedCommand.isBindEligible(obj) || obj.isBindValue())) {
            buffer.append(UNDEFINED_PARAM);
            preparedValues.add(obj);
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
        prepareCallBuffer.append("{ "); //$NON-NLS-1$

        List<Argument> params = exec.getArguments();

        //check whether a "?" is needed if there are returns
        boolean needQuestionMark = exec.getReturnType() != null;
        
        prepareCallBuffer.append(getSourceComment(exec));
        
        if(needQuestionMark){
            prepareCallBuffer.append("?="); //$NON-NLS-1$
        }

        prepareCallBuffer.append(" call ");//$NON-NLS-1$
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
	    		buffer.append(Tokens.LPAREN).append(nativeQuery).append(Tokens.RPAREN);
	    		if (obj.getCorrelationName() == null) {
	                buffer.append(Tokens.SPACE);
	    			if (useAsInGroupAlias()){
	                    buffer.append(AS).append(Tokens.SPACE);
	                }
	    		}
	    	}
		}
		super.appendBaseName(obj);
	}
	
	
}
