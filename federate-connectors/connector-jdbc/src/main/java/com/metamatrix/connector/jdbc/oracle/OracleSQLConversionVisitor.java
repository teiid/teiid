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
package com.metamatrix.connector.jdbc.oracle;

import java.sql.Time;
import java.util.Iterator;

import com.metamatrix.connector.jdbc.extension.SQLConversionVisitor;
import com.metamatrix.data.api.TypeFacility;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.ICompareCriteria;
import com.metamatrix.data.language.IElement;
import com.metamatrix.data.language.IExpression;
import com.metamatrix.data.language.IGroup;
import com.metamatrix.data.language.IInsert;
import com.metamatrix.data.language.ILanguageObject;
import com.metamatrix.data.language.ILimit;
import com.metamatrix.data.language.ILiteral;
import com.metamatrix.data.language.ISelect;
import com.metamatrix.data.language.ISetQuery.Operation;
import com.metamatrix.data.metadata.runtime.Element;
import com.metamatrix.data.metadata.runtime.MetadataID;

/**
 */
public class OracleSQLConversionVisitor extends SQLConversionVisitor {

    private final int MAX_SELECT_ALIAS_LENGTH = 30;
    
    private final int MAX_TABLE_ALIAS_LENGTH = 30;
    
    protected final static String DUAL = "DUAL"; //$NON-NLS-1$
    protected final static String ROWNUM = "ROWNUM"; //$NON-NLS-1$
    protected final static String SEQUENCE = ":SEQUENCE="; //$NON-NLS-1$
    protected final static String HINT_PREFIX = "/*+"; //$NON-NLS-1$
    
    
    /**
     * Overriden to check for Oracle SQL hints
     * @param obj ISelect object
     * @since 4.3
     */
    protected void visitSelect(ISelect obj) {
        buffer.append(SELECT).append(SPACE);
        buffer.append(addProcessComment());
        
        // Check for db hints
        Object payload = this.getExecutionContext().getExecutionPayload();
        if (payload instanceof String) {
            String payloadString = (String)payload;
            if (payloadString.startsWith(HINT_PREFIX)) {
                buffer.append(payloadString).append(SPACE);
            }
        }
        
        if (obj.isDistinct()) {
            buffer.append(DISTINCT).append(SPACE);
        }
        append(obj.getSelectSymbols());
    }
    
    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.IGroup)
     */
    protected boolean useAsInGroupAlias(){
        return false;
    }
    
    /**
     * Don't fully qualify elements if table = DUAL or element = ROWNUM or special stuff is packed into name in source value.
     *  
     * @see com.metamatrix.data.visitor.util.SQLStringVisitor#skipGroupInElement(java.lang.String, java.lang.String)
     * @since 5.0
     */
    protected String replaceElementName(String group, String element) {        

        // Check if the element was modeled as using a Sequence
        String elementTrueName = getElementTrueName(element);
        if (elementTrueName != null) {
            return group + DOT + elementTrueName;
        }
        
        // Check if the group name should be discarded
        if((group != null && group.equalsIgnoreCase(DUAL)) || element.equalsIgnoreCase(ROWNUM)) {
            // Strip group if group or element are pseudo-columns
            return element;
        }
        
        return null;
    }
    
    /**
     * Check if the element was modeled as using a Sequence to
     * generate a unique value for Inserting - strip off everything
     * except the element name.  Otherwise return null.
     * @param element
     * @return
     * @since 4.3
     */
    protected String getElementTrueName(String element) {
        
        int useIndex = element.indexOf(SEQUENCE);
        if (useIndex >= 0) {
            return element.substring(0, useIndex);  
        }
        
        return null;
    }

    /* 
     * @see com.metamatrix.connector.jdbc.extension.SQLConversionVisitor#translateLiteralTime(java.sql.Time)
     */
    protected String translateLiteralTime(Time timeValue) {
        return "{ts'1970-01-01 " + formatDateValue(timeValue) + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    private static final String DATE_TYPE = "DATE"; //$NON-NLS-1$
    
    public void visit(ICompareCriteria obj) {
        IExpression leftExpression = obj.getLeftExpression();
        append(leftExpression);
        buffer.append(SPACE);
        
        final int op = obj.getOperator();
        switch(op) {
            case ICompareCriteria.EQ: buffer.append(EQ); break;
            case ICompareCriteria.GE: buffer.append(GE); break;
            case ICompareCriteria.GT: buffer.append(GT); break;
            case ICompareCriteria.LE: buffer.append(LE); break;
            case ICompareCriteria.LT: buffer.append(LT); break;
            case ICompareCriteria.NE: buffer.append(NE); break;
        }
        buffer.append(SPACE);
        
        IExpression rightExpression = obj.getRightExpression();
        if(leftExpression instanceof IElement && rightExpression instanceof ILiteral && ((ILiteral)rightExpression).getType().equals(TypeFacility.RUNTIME_TYPES.TIMESTAMP) && ((ILiteral)rightExpression).getValue() != null) {
            translateStringToTimestampConversion(leftExpression, rightExpression);
            
        } else {
            append(rightExpression);           
        }        
    }
    
    /** 
     * If a column is modeled with name in source indicating that an Oracle Sequence should
     * be used to generate the value to insert, then pull the Sequence name out of the name
     * in source of the column, discard the (dummy) literal value from the user-entered value list,
     * and replace with the Sequence (as an IElement).
     * Implementation note: An IGroup will be used for the Sequence name, and an IElement will be used
     * for the Sequence operation (i.e. "nextVal").
     * This nasty kludge is brought to you by Tier 3.
     * @see com.metamatrix.connector.jdbc.extension.SQLConversionVisitor#visit(com.metamatrix.data.language.IInsert)
     * @since 4.3
     */
    public void visit(IInsert obj) {
        
        Iterator iter = obj.getElements().iterator();
        for (int i=0; i<obj.getElements().size(); i++) {
            
            IElement element = (IElement)iter.next();
            MetadataID metadataID = element.getMetadataID();
            if (metadataID != null) {
                String name = getName(metadataID);
                
                //Check whether a Sequence should be used to insert value for this column
                String upperName = name.toUpperCase();
                int index = upperName.indexOf(SEQUENCE);
                if (index >= 0) {
                    
                    String sequence = name.substring(index + SEQUENCE.length());
                    
                    int delimiterIndex = sequence.indexOf(DOT);
                    if (delimiterIndex >= 0) {
                        String sequenceGroupName = sequence.substring(0, delimiterIndex);
                        String sequenceElementName = sequence.substring(delimiterIndex + 1);
                            
                        ILanguageObject previousValue = (ILanguageObject)obj.getValues().get(i);
                        IGroup sequenceGroup = this.getLanguageFactory().createGroup(sequenceGroupName, null, null);
                        IElement sequenceElement = this.getLanguageFactory().createElement(sequenceElementName, sequenceGroup, null, previousValue.getClass());
                        
                        obj.getValues().set(i, sequenceElement);
                        
                    }
                }
                
            }
            
        }
        
        super.visit(obj);
    }

    protected void translateStringToTimestampConversion(IExpression leftExpression,
                                                      IExpression rightExpression) {
        String nativeType = DATE_TYPE;

        try {
            // Determine native type of left element
            IElement element = (IElement) leftExpression;
            MetadataID metadataID = element.getMetadataID();
            Element elem = (Element) super.metadata.getObject(metadataID);
            nativeType = elem.getNativeType();                
        } catch(ConnectorException e) {
            // ignore, use default
        }
        
        // Translate timestamp based on native type of compared element - Oracle will
        // only use an index for date or timestamp type columns if the proper function
        // is used.  The importer will import date types as type=DATE and timestamp types
        // as type=TIMESTAMP(0), etc.
        ILiteral timestamp = (ILiteral) rightExpression;
        if(nativeType != null && nativeType.equalsIgnoreCase(DATE_TYPE)) {
            buffer.append("to_date('"); //$NON-NLS-1$
            
            String tsValue = formatDateValue(timestamp.getValue());
            int decimalIndex = tsValue.lastIndexOf("."); //$NON-NLS-1$
            if(decimalIndex >= 0) {                
                buffer.append(tsValue.substring(0, decimalIndex));
            } else {
                buffer.append(tsValue);
            }
            buffer.append("','YYYY-MM-DD HH24:MI:SS')");//$NON-NLS-1$
        } else {
            buffer.append("to_timestamp('"); //$NON-NLS-1$
            buffer.append(formatDateValue(timestamp.getValue()));
            buffer.append("','YYYY-MM-DD HH24:MI:SS.FF')");//$NON-NLS-1$
        }
    }
    
    public void visit(ILimit obj) {
        //limits should be removed by the translator
    }
    
    /** 
     * @see com.metamatrix.data.visitor.util.SQLStringVisitor#appendSetOperation(com.metamatrix.data.language.ISetQuery.Operation)
     */
    @Override
    protected void appendSetOperation(Operation operation) {
        if (operation.equals(Operation.EXCEPT)) {
            buffer.append("MINUS"); //$NON-NLS-1$
        } else {
            super.appendSetOperation(operation);
        }
    }

    /** 
     * @see com.metamatrix.connector.jdbc.extension.SQLConversionVisitor#getMaxSelectAliasLength()
     * @since 4.3
     */
    protected int getMaxSelectAliasLength() {
        return MAX_SELECT_ALIAS_LENGTH;
    }

    /** 
     * @see com.metamatrix.connector.jdbc.extension.SQLConversionVisitor#getMaxTableAliasLength()
     * @since 4.3
     */
    protected int getMaxTableAliasLength() {
        return MAX_TABLE_ALIAS_LENGTH;
    }
}
