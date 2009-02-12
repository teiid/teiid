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

package com.metamatrix.jdbc.api;

import java.util.ArrayList;
import java.util.List;

/**
 * Implement a default display helper that can be used with the {@link TextOutputVisitor} 
 * and {@link XMLOutputVisitor}.  
 */
public class DefaultDisplayHelper implements DisplayHelper {

    private static final String OUTPUT_COLS = "outputCols"; //$NON-NLS-1$
    private static final String TYPE = "type"; //$NON-NLS-1$


    /* 
     * @see com.metamatrix.jdbc.api.DisplayHelper#getName(com.metamatrix.jdbc.api.PlanNode)
     */
    public String getName(PlanNode node) {
        return (String) node.getProperties().get(TYPE); 
    }

    /* 
     * @see com.metamatrix.jdbc.api.DisplayHelper#getDescription(com.metamatrix.jdbc.api.PlanNode)
     */
    public String getDescription(PlanNode node) {
        return ""; //$NON-NLS-1$
    }

    /* 
     * @see com.metamatrix.jdbc.api.DisplayHelper#getOrderedProperties(com.metamatrix.jdbc.api.PlanNode)
     */
    public List getOrderedProperties(PlanNode node) {
        List props = new ArrayList(node.getProperties().keySet());
        props.remove(TYPE);
        if(props.contains(OUTPUT_COLS)) {
            props.remove(OUTPUT_COLS);
            props.add(0, OUTPUT_COLS);            
        }
        return props;
    }

    /* 
     * @see com.metamatrix.jdbc.api.DisplayHelper#getPropertyName(java.lang.String)
     */
    public String getPropertyName(String property) {
        return property;
    }

    /* 
     * @see com.metamatrix.jdbc.api.DisplayHelper#setMaxDescriptionLength(int)
     */
    public void setMaxDescriptionLength(int length) {

    }

}
