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

package org.teiid.query.optimizer.xml;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.mapping.xml.MappingAttribute;
import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.MappingElement;
import org.teiid.query.mapping.xml.MappingSourceNode;
import org.teiid.query.mapping.xml.MappingVisitor;
import org.teiid.query.mapping.xml.Navigator;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;



/** 
 * This visitor resolves the "element" names on the mapping nodes to the planned
 * sources defined at same or parent nodes. 
 */
public class NameInSourceResolverVisitor extends MappingVisitor {
    XMLPlannerEnvironment planEnv;
    
    NameInSourceResolverVisitor(XMLPlannerEnvironment planEnv) {
        this.planEnv = planEnv;
    }
    
    public void visit(MappingSourceNode element) {
        if (element.getAliasResultName() == null) {
            return;
        }
        
        Map symbols = element.getSymbolMap();
        
        List<ElementSymbol> elements = new LinkedList<ElementSymbol>();
        
        for (Iterator i = symbols.values().iterator(); i.hasNext();) {
            Object symbol = i.next();
            if (symbol instanceof ElementSymbol) {
                elements.add((ElementSymbol)symbol);
            }
        }
        
        //fix recusive references
        Map fixedMap = QueryUtil.createSymbolMap(new GroupSymbol(element.getActualResultSetName()), element.getAliasResultName(), elements);
        
        updateSymbolMap(element, fixedMap);
        
        symbols.putAll(fixedMap);
    }
    
    public void visit(MappingAttribute attribute) {
        String nameInSource = attribute.getNameInSource();
        if (nameInSource != null) {
            
            MappingSourceNode sourceNode = attribute.getSourceNode();
            ElementSymbol symbol = resolveElementSymbol(nameInSource, sourceNode);            
            
            attribute.setElementSymbol(symbol);
        }
    }
    
    public void visit(MappingElement element) {
        String nameInSource = element.getNameInSource();
        if (nameInSource != null) {
            MappingSourceNode sourceNode = element.getSourceNode();
            ElementSymbol symbol = resolveElementSymbol(nameInSource, sourceNode);
            
            element.setElementSymbol(symbol);
        }
    }
 
    private ElementSymbol resolveElementSymbol(String elementName, MappingSourceNode sourceNode) {
        try {
            QueryMetadataInterface metadata = this.planEnv.getGlobalMetadata(); 
            
            ElementSymbol symbol = sourceNode.getMappedSymbol(new ElementSymbol(elementName));
            
            Object elementID = metadata.getElementID(symbol.getName());
            symbol.setMetadataID(elementID);
   
            Object groupID = metadata.getGroupIDForElementID(elementID);
            String groupName = metadata.getFullName(groupID);
            GroupSymbol groupSymbol = new GroupSymbol(groupName);
            groupSymbol.setMetadataID(groupID);
   
            symbol.setGroupSymbol(groupSymbol);
            symbol.setType(DataTypeManager.getDataTypeClass(metadata.getElementType(symbol.getMetadataID())));
            return symbol;
        } catch (QueryMetadataException e) {
            throw new TeiidRuntimeException(e);
        } catch (TeiidComponentException e) {
            throw new TeiidRuntimeException(e);
        }
    }
    
    /**
     * Resolve all the "NameInSource" property nodes (element names), with the their results
     * set queries. 
     * @param doc
     * @param planEnv
     */
    public static void resolveElements(MappingDocument doc, XMLPlannerEnvironment planEnv) 
        throws QueryMetadataException, TeiidComponentException {

        NameInSourceResolverVisitor real = new NameInSourceResolverVisitor(planEnv);
        
        try {
            MappingVisitor visitor = new Navigator(true, real);
            doc.acceptVisitor(visitor);
        } catch (TeiidRuntimeException e) {
            if (e.getCause() instanceof QueryMetadataException) {
                throw (QueryMetadataException)e.getCause();
            }
            else if (e.getCause() instanceof TeiidComponentException) {
                throw (TeiidComponentException)e.getCause();
            }
            throw e;
        }
    }
    
    static void updateSymbolMap(MappingSourceNode source,
                                Map target) {
        for (Iterator i = source.getSymbolMap().entrySet().iterator(); i.hasNext();) {
            Map.Entry entry = (Map.Entry)i.next();
            Object newValue = target.get(entry.getValue());

            if (newValue != null) {
                entry.setValue(newValue);
            }
        }

        source.updateSymbolMapDependentValues();
}
}
