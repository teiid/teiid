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

package org.teiid.jdbc.plan;

import java.util.ArrayList;
import java.util.List;

/**
 * Implement a default display helper that can be used with the {@link TextOutputVisitor} 
 * and {@link XMLOutputVisitor}.  
 */
public class DefaultDisplayHelper implements DisplayHelper {

    public String getName(PlanNode node) {
        return (String)node.getProperties().get(PlanNode.TYPE); 
    }

    public String getDescription(PlanNode node) {
        return ""; //$NON-NLS-1$
    }

    public List<String> getOrderedProperties(PlanNode node) {
        List<String> props = new ArrayList<String>(node.getProperties().keySet());
        props.remove(PlanNode.TYPE);
        if(props.contains(PlanNode.OUTPUT_COLS)) {
            props.remove(PlanNode.OUTPUT_COLS);
            props.add(0, PlanNode.OUTPUT_COLS);            
        }
        return props;
    }

    public String getPropertyName(String property) {
        return property;
    }

    public void setMaxDescriptionLength(int length) {

    }

}
