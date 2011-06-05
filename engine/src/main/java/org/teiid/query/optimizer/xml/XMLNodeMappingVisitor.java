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

import java.util.Collection;

import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.query.QueryPlugin;
import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.MappingNode;
import org.teiid.query.mapping.xml.MappingSourceNode;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.visitor.AbstractSymbolMappingVisitor;


/**
 * This visitor is able to map symbols based on the XML document model:
 * given a symbol representing one of the nodes of the document which is
 * mapped, this object will find the Symbol representing the relational
 * element it is mapped to.
 */
public class XMLNodeMappingVisitor extends AbstractSymbolMappingVisitor {

	private MappingNode rootNode;
    private QueryMetadataInterface metadata;

    /**
     * Constructor for XMLNodeMappingVisitor.
     */
    public XMLNodeMappingVisitor(MappingDocument rootNode, QueryMetadataInterface metadata) {
        this.rootNode = rootNode;
        this.metadata = metadata;
    }
    
    @Override
    protected boolean createAliases() {
    	return false; //xml style selects do not have aliases
    }
    
    /**
     * @see AbstractSymbolMappingVisitor#getMappedSymbol(Symbol)
     */
    protected Symbol getMappedSymbol(Symbol symbol) {
        try {
        	Object metadataId = null;
        	Object groupId = null;
	    	if (symbol instanceof GroupSymbol) {
	    		GroupSymbol groupSymbol = (GroupSymbol)symbol;
	    		metadataId = groupSymbol.getMetadataID();
	    		groupId = metadataId;
	    	} else {
				ElementSymbol element = (ElementSymbol) symbol;
				metadataId = element.getMetadataID();
				groupId = element.getGroupSymbol().getMetadataID();
	    	}
	    	boolean xml = metadata.isXMLGroup(groupId);
	    	if (!xml) {
    			return symbol;
	    	}
	    	String path = metadata.getFullName(metadataId).toUpperCase();
	
    		// Find mapping node for specified path
    		MappingNode node = MappingNode.findNode(rootNode, path); 
    		if(node == null) { 
    			return null;
    		}
    		MappingSourceNode msn = node.getSourceNode();
			if (msn == null) {
				return null;
			}
    		if (symbol instanceof GroupSymbol) {
    			GroupSymbol gs = msn.getMappedSymbol(new GroupSymbol(msn.getResultName()));
    			return gs;
    		} 
    		// Construct a new element node based on mapping node reference
    		String symbolName = node.getNameInSource();
    		if (symbolName == null){
    			return null;
    		}
			ElementSymbol es = msn.getMappedSymbol(new ElementSymbol(symbolName));
			return es;
        } catch (TeiidException err) {
            throw new TeiidRuntimeException(err);
        } 
    }
    
    /**
     * Convert the criteria from names using document identifiers to names using
     * result set (i.e. mapping class) identifiers.
     * @param simpleCrit Criteria to convert
     * @param rootNode Root of mapping node tree
     * @return Criteria Converted criteria
     * @throws QueryPlannerException if simpleCrit has a XML document model element
     * that is not mapped to data in a mapping class
     */
    public static Criteria convertCriteria(Criteria simpleCrit, MappingDocument rootNode, QueryMetadataInterface metadata)
    throws QueryPlannerException, TeiidComponentException{
        return convertObject((Criteria)simpleCrit.clone(), rootNode, metadata, true);
    }

    public static <T extends LanguageObject> T convertObject(T object, MappingDocument rootNode, QueryMetadataInterface metadata, boolean deep)
    throws QueryPlannerException, TeiidComponentException{
        //Don't want to do deep visiting
        XMLNodeMappingVisitor mappingVisitor = new XMLNodeMappingVisitor(rootNode, metadata);
        try {
            PreOrPostOrderNavigator.doVisit(object, mappingVisitor, PreOrPostOrderNavigator.POST_ORDER, deep);
        } catch (TeiidRuntimeException e) {
            Throwable child = e.getChild();
            
            if (child instanceof TeiidComponentException) {
                throw (TeiidComponentException)child;
            }
            
            throw new TeiidComponentException(child);
        }

        Collection unmappedSymbols = mappingVisitor.getUnmappedSymbols();
        if (unmappedSymbols != null && unmappedSymbols.size() > 0){
            throw new QueryPlannerException("ERR.015.004.0046", QueryPlugin.Util.getString("ERR.015.004.0046", new Object[] {unmappedSymbols, object})); //$NON-NLS-1$ //$NON-NLS-2$
        }

        return object;
    }
        
}
