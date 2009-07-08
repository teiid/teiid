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

package org.teiid.connector.metadata;

import com.metamatrix.core.util.ArgCheck;


/** 
 * This object is used for construting match patterns used to query index files
 * containing metadata information. This object can also be used to post process
 * results after querying index files. This object directly mapps to a single criteria
 * in a SQL query.
 * 
 * @since 4.3
 */
public class MetadataLiteralCriteria implements MetadataSearchCriteria {
    
    private final String fieldName;
    private final Object fieldValue;
    private String fieldFunction;
    private String valueFunction;
    
    /**
     * Constructor MetadataLiteralCriteria
     * @param fieldName The name of field used on criteria which gets matched against a literal.
     * @param fieldValue The value of field used on criteria which is a literal used as part of seach criteria.
     * @since 4.3
     */
    public MetadataLiteralCriteria(final String fieldName, final Object fieldValue) {
        ArgCheck.isNotNull(fieldName);
        
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;        
    }

    /** 
     * Get the name of the function applied on the field part of criteria.
     * @return returns the fieldFunction.
     * @since 4.3
     */
    public String getFieldFunction() {
        return this.fieldFunction;
    }

    /** 
     * Set the name of the function applied on the field part of criteria.
     * @param fieldFunction The fieldFunction to set.
     * @since 4.3
     */
    public void setFieldFunction(String fieldFunction) {
        this.fieldFunction = fieldFunction;
    }

    /** 
     * Get the name of field used on criteria which gets matched against a literal.
     * @return returns the fieldName.
     * @since 4.3
     */
    public String getFieldName() {
        return this.fieldName;
    }
    
    /** 
     * Get the value of field used on criteria which is a literal used as part of seach criteria.
     * @return returns the fieldValue.
     * @since 4.3
     */
    public Object getFieldValue() {
        return this.fieldValue;
    }
    
    /**
     * Check if this criteria evaluates to a false criteria. 
     * @return true if this evaluates to a false criteria else false
     * @since 4.3
     */
    public boolean isFalseCriteria() {
        Object evaluatedValue = getEvaluatedValue();
        if(evaluatedValue instanceof String && this.fieldFunction != null) {
            String stringValue = evaluatedValue.toString();
            if(this.fieldFunction.equalsIgnoreCase("UPPER") || this.fieldFunction.equalsIgnoreCase("UCASE")) { //$NON-NLS-1$ //$NON-NLS-2$
                return !stringValue.equals(stringValue.toUpperCase());
            }
            if(this.fieldFunction.equalsIgnoreCase("LOWER") || this.fieldFunction.equalsIgnoreCase("LCASE")) { //$NON-NLS-1$ //$NON-NLS-2$
                return !stringValue.equals(stringValue.toLowerCase());
            }
        }
        return false;
    }
    
    /**
     * Get the value of the literal field used in the criteria with any function on it evaluated. 
     * @return returns the fieldValue.
     * @since 4.3
     */
    public Object getEvaluatedValue() {
        if(this.valueFunction != null) {
            if(this.fieldValue instanceof String) {
                if(this.valueFunction.equalsIgnoreCase("UPPER") || this.valueFunction.equalsIgnoreCase("UCASE")) { //$NON-NLS-1$ //$NON-NLS-2$
                    return this.fieldValue.toString().toUpperCase();
                }
                if(this.valueFunction.equalsIgnoreCase("LOWER") || this.valueFunction.equalsIgnoreCase("LCASE")) { //$NON-NLS-1$ //$NON-NLS-2$
                    return this.fieldValue.toString().toLowerCase();
                }
            }
        }
        return this.getFieldValue();        
    }
    
    /** 
     * Get the name of the function applied on the literal part of criteria.
     * @return returns the valueFunction.
     * @since 4.3
     */
    public String getValueFunction() {
        return this.valueFunction;
    }

    /** 
     * Set the name of the function applied on the literal part of criteria.
     * @param valueFunction The valueFunction to set.
     * @since 4.3
     */
    public void setValueFunction(String valueFunction) {
        this.valueFunction = valueFunction;
    }
    
    /**
     * Return true if any case functions are involved on the fieldName of this criteria 
     * @return true if there are any functions else false.
     * @since 4.3
     */
    public boolean hasFieldWithCaseFunctions() {
        if(this.fieldFunction != null) {
            if(this.fieldFunction.equalsIgnoreCase("UPPER") || this.fieldFunction.equalsIgnoreCase("UCASE") || //$NON-NLS-1$ //$NON-NLS-2$
                            this.fieldFunction.equalsIgnoreCase("LOWER") || this.fieldFunction.equalsIgnoreCase("LCASE")) { //$NON-NLS-1$ //$NON-NLS-2$
                return true;
            }
        }
        return false;
    }    
    
}