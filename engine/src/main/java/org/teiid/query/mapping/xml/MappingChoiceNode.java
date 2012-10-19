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

package org.teiid.query.mapping.xml;

import java.util.Iterator;
import java.util.List;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.query.QueryPlugin;




/** 
 * Node that describes a <b>choice</b> node in an XML Scheam Mapping document
 * 
 * This allows only the criteria nodes to be added to choice node.
 */
public class MappingChoiceNode extends MappingBaseNode {

    public MappingChoiceNode() {
        this(false);
    }
    
    public MappingChoiceNode(boolean exceptionOnDefault) {
        //setProperty(MappingNodeConstants.Properties.NAME, "{ChoiceNode}"); //$NON-NLS-1$
        setProperty(MappingNodeConstants.Properties.NODE_TYPE, MappingNodeConstants.CHOICE);
        setProperty(MappingNodeConstants.Properties.EXCEPTION_ON_DEFAULT, Boolean.valueOf(exceptionOnDefault));
    }

    public void acceptVisitor(MappingVisitor visitor) {
        visitor.visit(this);
    }
    
    /**
     * Adds the Element Node to the current Element node and returns the 
     * child node added to the current node.
     */
    public MappingCriteriaNode addCriteriaNode(MappingCriteriaNode node) {
        String criteria = node.getCriteria();
        if (criteria == null && !node.isDefault()) {
            throw new RuntimeException(QueryPlugin.Util.getString("NoCriteria")); //$NON-NLS-1$
        }
        
        addChild(node);        
        return node;
    } 
    
    public MappingCriteriaNode getDefaultNode() {
        List critNodes = getChildren();
        for (final Iterator i = critNodes.iterator(); i.hasNext();) {
            final MappingCriteriaNode node= (MappingCriteriaNode)i.next();
            if (node.isDefault()) {
                return node;
            }
        } // for
        return null;
    }           
    
    public boolean throwExceptionOnDefault() {
        Boolean exceptionOnDefault = (Boolean)getProperty(MappingNodeConstants.Properties.EXCEPTION_ON_DEFAULT);
        if (exceptionOnDefault != null) {
            return exceptionOnDefault.booleanValue();   
        }
        return false;
    }

    public MappingAllNode addAllNode(MappingAllNode elem) {
         throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30452, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30452));
    }

    public MappingChoiceNode addChoiceNode(MappingChoiceNode elem) {
         throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30452, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30452));
    }

    public MappingSequenceNode addSequenceNode(MappingSequenceNode elem) {
         throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30452, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30452));
    }
    
    public MappingElement addChildElement(MappingElement elem) {
         throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30452, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30452));
    }    
    
    public MappingSourceNode addSourceNode(MappingSourceNode elem) {
         throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30452, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30452));
    }     
}
