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
package com.metamatrix.connector.jdbc.extension;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.FieldPosition;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.TypeFacility;
import com.metamatrix.connector.jdbc.JDBCPropertyNames;
import com.metamatrix.connector.language.IBulkInsert;
import com.metamatrix.connector.language.IDelete;
import com.metamatrix.connector.language.IElement;
import com.metamatrix.connector.language.IFunction;
import com.metamatrix.connector.language.IInsert;
import com.metamatrix.connector.language.ILanguageFactory;
import com.metamatrix.connector.language.ILanguageObject;
import com.metamatrix.connector.language.ILiteral;
import com.metamatrix.connector.language.IParameter;
import com.metamatrix.connector.language.IProcedure;
import com.metamatrix.connector.language.IQuery;
import com.metamatrix.connector.language.IUpdate;
import com.metamatrix.connector.visitor.util.SQLStringVisitor;

/**
 * This visitor takes an ICommand and does DBMS-specific conversion on it
 * to produce a SQL String.  This class is expected to be subclassed.
 * Specialized instances of this class can be gotten from a SQL Translator
 * {@link SQLTranslator#getTranslationVisitor() using this method}.
 */
public class SQLConversionVisitor extends SQLStringVisitor{

    private static DecimalFormat decimalFormatter = 
        new DecimalFormat("#############################0.0#############################"); //$NON-NLS-1$    
    private static double SCIENTIC_LOW = Math.pow(10, -3);
    private static double SCIENTIC_HIGH = Math.pow(10, 7);
    private static final MessageFormat COMMENT = new MessageFormat("/*metamatrix sessionid:{0}, requestid:{1}.{2}*/ "); //$NON-NLS-1$
    private boolean useComment = false;
    
    private Map modifiers;
    private ExecutionContext context;
    private ILanguageFactory languageFactory;
    private TimeZone databaseTimeZone;

    private int execType = TranslatedCommand.EXEC_TYPE_QUERY;
    private int stmtType = TranslatedCommand.STMT_TYPE_STATEMENT;
    
    private List preparedValues = new ArrayList();
    private List preparedTypes = new ArrayList();
    
    public SQLConversionVisitor() {
        super();
    }

    /**
     * @see com.metamatrix.connector.visitor.util.SQLStringVisitor#visit(com.metamatrix.connector.language.IInsert)
     */
    public void visit(IInsert obj) {
        this.execType = TranslatedCommand.EXEC_TYPE_UPDATE;
        super.visit(obj);
    }

    public void visit(IBulkInsert obj) {
        this.stmtType = TranslatedCommand.STMT_TYPE_PREPARED_STATEMENT;

        super.visit(obj);
        
        for (int i = 0; i < obj.getElements().size(); i++) {
            IElement element = (IElement) obj.getElements().get(i);
            this.preparedTypes.add(element.getType());
        }

        this.preparedValues = obj.getRows();
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
                    valuesbuffer.append(decimalFormatter.format(obj));
                }
                else {
                    valuesbuffer.append(obj);
                }
            } else if(type.equals(TypeFacility.RUNTIME_TYPES.BOOLEAN)) {
                valuesbuffer.append(translateLiteralBoolean((Boolean)obj));
            } else if(type.equals(TypeFacility.RUNTIME_TYPES.TIMESTAMP)) {
                valuesbuffer.append(translateLiteralTimestamp((Timestamp)obj));
            } else if(type.equals(TypeFacility.RUNTIME_TYPES.TIME)) {
                valuesbuffer.append(translateLiteralTime((Time)obj));
            } else if(type.equals(TypeFacility.RUNTIME_TYPES.DATE)) {
                valuesbuffer.append(translateLiteralDate((java.sql.Date)obj));
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
     * @see com.metamatrix.connector.visitor.util.SQLStringVisitor#visit(com.metamatrix.connector.language.IUpdate)
     */
    public void visit(IUpdate obj) {
        this.execType = TranslatedCommand.EXEC_TYPE_UPDATE;
        super.visit(obj);
    }

    /**
     * @see com.metamatrix.connector.visitor.util.SQLStringVisitor#visit(com.metamatrix.connector.language.IQuery)
     */
    public void visit(IQuery obj) {
        this.execType = TranslatedCommand.EXEC_TYPE_QUERY;
        super.visit(obj);
    }

    /**
     * @see com.metamatrix.connector.visitor.util.SQLStringVisitor#visit(com.metamatrix.connector.language.IProcedure)
     */
    public void visit(IProcedure obj) {
        this.execType = TranslatedCommand.EXEC_TYPE_EXECUTE;
        this.stmtType = TranslatedCommand.STMT_TYPE_CALLABLE_STATEMENT;
        /*
         * preparedValues is now a list of procedure params instead of just values
         */
        this.preparedValues = obj.getParameters();
        super.buffer.append(generateSqlForStoredProcedure(obj));
    }

    /**
     * @see com.metamatrix.connector.visitor.util.SQLStringVisitor#visit(com.metamatrix.connector.language.IDelete)
     */
    public void visit(IDelete obj) {
        this.execType = TranslatedCommand.EXEC_TYPE_UPDATE;
        super.visit(obj);
    }

    /**
     * @see com.metamatrix.connector.visitor.util.SQLStringVisitor#visit(com.metamatrix.connector.language.IFunction)
     */
    public void visit(IFunction obj) {
        if(this.modifiers != null) {
            FunctionModifier functionModifier = (FunctionModifier)this.modifiers.get(obj.getName().toLowerCase());
            if(functionModifier != null) {
                List parts = functionModifier.translate(obj);
                
                // null means the FunctionModifier will rely on default translation
                if (parts != null) {
                    Iterator iter = parts.iterator();
                    while(iter.hasNext()) {
                        Object part = iter.next();
                        if(part instanceof String) {
                            buffer.append(part);
                        } else {
                            append((ILanguageObject)part);
                        }
                    }
                    return;
                } 
            } 
        } 
        super.visit(obj);
    }

    /**
     * @see com.metamatrix.connector.visitor.util.SQLStringVisitor#visit(com.metamatrix.connector.language.ILiteral)
     */
    public void visit(ILiteral obj) {
        if (this.stmtType == TranslatedCommand.STMT_TYPE_PREPARED_STATEMENT && obj.isBindValue()) {
            buffer.append(UNDEFINED_PARAM);
            preparedValues.add(obj.getValue());
            preparedTypes.add(obj.getType());
        } else {
            translateSQLType(obj.getType(), obj.getValue(), buffer);
        }
    }

    /**
     * Subclasses should override this method to provide a different sql translation
     * of the literal boolean value.  By default, a boolean literal is represented as:
     * <code>'0'</code> or <code>'1'</code>.
     * @param booleanValue Boolean value, never null
     * @return Translated string
     */
    protected String translateLiteralBoolean(Boolean booleanValue) {
        if(booleanValue.booleanValue()) {
            return "1"; //$NON-NLS-1$
        }
        return "0"; //$NON-NLS-1$
    }

    /**
     * Subclasses should override this method to provide a different sql translation
     * of the literal date value.  By default, a date literal is represented as:
     * <code>{d'2002-12-31'}</code>
     * @param dateValue Date value, never null
     * @return Translated string
     */
    protected String translateLiteralDate(java.sql.Date dateValue) {
        return "{d'" + formatDateValue(dateValue) + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Subclasses should override this method to provide a different sql translation
     * of the literal time value.  By default, a time literal is represented as:
     * <code>{t'23:59:59'}</code>
     * @param timeValue Time value, never null
     * @return Translated string
     */
    protected String translateLiteralTime(Time timeValue) {
        return "{t'" + formatDateValue(timeValue) + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Subclasses should override this method to provide a different sql translation
     * of the literal timestamp value.  By default, a timestamp literal is
     * represented as: <code>{ts'2002-12-31 23:59:59'}</code>.
     * @param timestampValue Timestamp value, never null
     * @return Translated string
     */
    protected String translateLiteralTimestamp(Timestamp timestampValue) {
        return "{ts'" + formatDateValue(timestampValue) + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     * Format the dateObject (of type date, time, or timestamp) into a string
     * using the DatabaseTimeZone format.
     * @param dateObject
     * @return Formatted string
     */
    protected String formatDateValue(Object dateObject) {
        if(this.databaseTimeZone == null) {
            return dateObject.toString();
        }
        
//System.out.println("!!! translating timestamp value " + dateObject + " (" + ((java.util.Date)dateObject).getTime() + " in " + this.databaseTimeZone);        
        
        if(dateObject instanceof Timestamp) {
            SimpleDateFormat timestampFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //$NON-NLS-1$
            timestampFormatter.setTimeZone(this.databaseTimeZone);

            Timestamp ts = (Timestamp) dateObject;                  
            String nanoStr = "" + (1000000000L + ts.getNanos()); //$NON-NLS-1$
            while(nanoStr.length() > 2 && nanoStr.charAt(nanoStr.length()-1) == '0') {
                nanoStr = nanoStr.substring(0, nanoStr.length()-1);
            }
            String tsStr = timestampFormatter.format(ts) + "." + nanoStr.substring(1); //$NON-NLS-1$
            
//System.out.println("!!!   returning " + tsStr);            
            
            return tsStr;
                    
        } else if(dateObject instanceof java.sql.Date) {
            SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd"); //$NON-NLS-1$
            dateFormatter.setTimeZone(this.databaseTimeZone);
            return dateFormatter.format((java.sql.Date)dateObject);
            
        } else if(dateObject instanceof Time) {
            SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm:ss"); //$NON-NLS-1$
            timeFormatter.setTimeZone(this.databaseTimeZone);
            return timeFormatter.format((java.sql.Time)dateObject);
            
        } else {
            return dateObject.toString();
        }       
    }    

    /**
     */
    public void setFunctionModifiers(Map modifiers) {
        this.modifiers = modifiers;
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

    public void setProperties(Properties props) {
        String useBindVariables = props.getProperty(JDBCPropertyNames.USE_BIND_VARIABLES, Boolean.FALSE.toString());
        if (useBindVariables.equals(Boolean.TRUE.toString())) {
            this.stmtType = TranslatedCommand.STMT_TYPE_PREPARED_STATEMENT;
        }   
        
        String useCommentInSource = props.getProperty(JDBCPropertyNames.USE_COMMENTS_SOURCE_QUERY);
        if (useCommentInSource != null) {
            this.useComment = Boolean.valueOf(useCommentInSource).booleanValue();
        }
    }
    
    /**
     * inserting the comments is the source SQL supported or not by the 
     * source data source. By default it is turned on; user has choice to
     * turn off by setting the connector property; the data base source has
     * option to turn off by overloading this. 
     * @return true if yes; false otherwise.
     */
    protected boolean supportsComments() {
        return this.useComment;
    }
    
    static final FieldPosition FIELD_ZERO = new FieldPosition(0);
    protected String addProcessComment() {
        if (supportsComments() && this.context != null) {
            return COMMENT.format(new Object[] {this.context.getConnectionIdentifier(), this.context.getRequestIdentifier(), this.context.getPartIdentifier()});
        }
        return super.addProcessComment(); 
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
            if(param.getDirection() == IParameter.RETURN){
                needQuestionMark = true;
                break;
            }
        }
        
        prepareCallBuffer.append(addProcessComment());
        
        if(needQuestionMark){
            prepareCallBuffer.append("?="); //$NON-NLS-1$
        }

        prepareCallBuffer.append(" call ");//$NON-NLS-1$
        prepareCallBuffer.append(exec.getMetadataID() != null ? getName(exec.getMetadataID()) : exec.getProcedureName());
        prepareCallBuffer.append("("); //$NON-NLS-1$

        int numberOfParameters = 0;
        iter = params.iterator();
        while(iter.hasNext()){
            IParameter param = (IParameter)iter.next();
            if(param.getDirection() == IParameter.IN || param.getDirection() == IParameter.OUT || param.getDirection() == IParameter.INOUT){
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
     * @param factory
     */
    public void setLanguageFactory(ILanguageFactory factory) {
        languageFactory = factory;
    }

    /**
     * @return
     */
    public ILanguageFactory getLanguageFactory() {
        return languageFactory;
    }

    public void setDatabaseTimeZone(TimeZone zone) {
        databaseTimeZone = zone;
    }

    protected TimeZone getDatabaseTimeZone() {
        return databaseTimeZone;
    }
    
    /** 
     * @return the execType
     */
    protected int getExecType() {
        return this.execType;
    }
    
    /** 
     * @return the preparedValues
     */
    List getPreparedValues() {
        return this.preparedValues;
    }
    
    /** 
     * @return the preparedValues
     */
    List getPreparedTypes() {
        return this.preparedTypes;
    }
    
    /** 
     * @return the stmtType
     */
    int getStmtType() {
        return this.stmtType;
    }
    
    protected void setStmtType(int stmtType) {
        this.stmtType = stmtType;
    }
            
}
