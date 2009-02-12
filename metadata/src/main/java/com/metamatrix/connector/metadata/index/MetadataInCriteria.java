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

package com.metamatrix.connector.metadata.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import com.metamatrix.core.util.ArgCheck;


/** 
 * @since 4.3
 */
public class MetadataInCriteria implements MetadataSearchCriteria {

    private final String fieldName;
    private final Collection fieldValues;
    private String fieldFunction;
    
    /**
     * Constructor MetadataInCriteria
     * @param fieldName The name of field used on criteria which gets matched against a literal.
     * @param fieldValue The value of field used on criteria which is a literal used as part of seach criteria.
     * @since 4.3
     */
    public MetadataInCriteria(final String fieldName, final Collection fieldValues) {
        ArgCheck.isNotNull(fieldName);
        
        this.fieldName = fieldName;
        this.fieldValues = fieldValues;        
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
    public Collection getFieldValues() {
        return this.fieldValues;
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

    /**
     * Get a collection of MetadataLiteralCriteria objects that represent this
     * MetadataInCriteria object.  
     * @return Collection of MetadataLiteralCriteria
     * @since 4.3
     */
    public Collection getLiteralCriteria() {
        Collection litCriteria = new ArrayList();
        for(final Iterator iter = getFieldValues().iterator(); iter.hasNext();) {
            MetadataLiteralCriteria literalCrit = new MetadataLiteralCriteria(this.fieldName, iter.next());
            literalCrit.setFieldFunction(this.fieldFunction);
            litCriteria.add(literalCrit);
        }
        return litCriteria;
    }
}