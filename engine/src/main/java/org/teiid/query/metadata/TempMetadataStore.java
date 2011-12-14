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

package org.teiid.query.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.teiid.query.metadata.TempMetadataID.Type;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.Symbol;


/**
 * Store for temporary metadata discovering while resolving a query.
 */
public class TempMetadataStore implements Serializable {

	private static final long serialVersionUID = 4055072385672022478L;
	// UPPER CASE TEMP GROUP NAME --> TempMetadataID for group
    private NavigableMap<String, TempMetadataID> tempGroups;     
    
    /**
     * Constructor for TempMetadataStore.
     */
    public TempMetadataStore() {
        this(new TreeMap<String, TempMetadataID>(String.CASE_INSENSITIVE_ORDER));
    }

    /**
     * Constructor for TempMetadataStore that takes a set of data to use.  If
     * the parameter is null, a new empty Map will beused instead.
     * @param data Map of upper case group name to group TempMetadataID object
     */
    public TempMetadataStore(NavigableMap<String, TempMetadataID> data) {
        if (data == null) {
            tempGroups = new TreeMap<String, TempMetadataID>(String.CASE_INSENSITIVE_ORDER);
        } else {
            tempGroups = data;
        }
    }
    
    public TempMetadataStore clone() {
    	TreeMap<String, TempMetadataID> clone = new TreeMap<String, TempMetadataID>(String.CASE_INSENSITIVE_ORDER);
    	clone.putAll(tempGroups);
    	return new TempMetadataStore(clone);
    }

    /**
     * Get all temp group and element metadata
     * @param data Map of upper case group name to group TempMetadataID object
     */
    public NavigableMap<String, TempMetadataID> getData() {
        return this.tempGroups;
    }
   
    /**
     * Add a temp group and all it's elements
     * @param tempGroup Name of temp group
     * @param tempSymbols List of ElementSymbol in position order
     */
    public TempMetadataID addTempGroup(String tempGroup, List tempSymbols) { 
        return addTempGroup(tempGroup, tempSymbols, true);
    }

    /**
     * Add a temp group and all it's elements
     * @param tempGroup Name of temp group
     * @param tempSymbols List of ElementSymbol in position order
     * @param isVirtual whether or not the group is a virtual group
     */
    public TempMetadataID addTempGroup(String tempGroup, List tempSymbols, boolean isVirtual) { 
        return addTempGroup(tempGroup, tempSymbols, isVirtual, false);
    }
    
    /**
     * Add a temp group and all it's elements
     * @param tempGroup Name of temp group
     * @param tempGroupDefinition optional definition if the tempGroup 
     * param is the aliased name of a group
     * @param tempSymbols List of ElementSymbol in position order
     * @param isVirtual whether or not the group is a virtual group
     * @param isTempTable whether or not the group is a temporary table
     */
    public TempMetadataID addTempGroup(String tempGroup, List<? extends Expression> tempSymbols, boolean isVirtual, boolean isTempTable) { 
        // Add the temporary group
        List<TempMetadataID> elementIDs = new ArrayList<TempMetadataID>(tempSymbols.size());
        
        for (Expression symbol : tempSymbols) {
            TempMetadataID elementID = createElementSymbol(tempGroup, symbol, isTempTable);
        
            elementIDs.add(elementID);
        }

        // Create group ID
        TempMetadataID groupID = new TempMetadataID(tempGroup, elementIDs, isVirtual?Type.VIRTUAL:Type.TEMP);
        this.tempGroups.put(tempGroup, groupID);
        return groupID;
    }

    private TempMetadataID createElementSymbol(String tempName, Expression symbol, boolean isTempTable) {
        // Create new element name
        String elementName = tempName + Symbol.SEPARATOR + Symbol.getShortName(symbol);
        
        Object metadataID = null;
        
        if (symbol instanceof AliasSymbol) {
            AliasSymbol as = (AliasSymbol)symbol;
            symbol = as.getSymbol();
        }
        
        //the following allows for original metadata ids to be determined for proc inputs
        if (symbol instanceof ExpressionSymbol) {
            Expression expr = ((ExpressionSymbol)symbol).getExpression();
            if (expr instanceof Reference) {
                expr = ((Reference)expr).getExpression();
            } 
            if (expr instanceof ElementSymbol) {
                symbol = expr;
            }
        }
        
        if (symbol instanceof ElementSymbol) {
            metadataID = ((ElementSymbol)symbol).getMetadataID();
        }
        
        while (metadataID != null && metadataID instanceof TempMetadataID) {
            metadataID = ((TempMetadataID)metadataID).getOriginalMetadataID();
        }
        
        TempMetadataID elementID = new TempMetadataID(elementName, symbol.getType(), metadataID);
        elementID.setTempTable(isTempTable);
        return elementID;
    }
    
    /**
     * Add a element symbol to the already created temp group. If added successfully then it will 
     * return the metadata id for the added element. otherwise it will return null.
     * @param tempGroup - temp group name
     * @param symbol - element to be added
     * @return metadata id.
     */
    public TempMetadataID addElementSymbolToTempGroup(String tempGroup, Expression symbol) {
        TempMetadataID groupID = this.tempGroups.get(tempGroup);
        if (groupID != null) {
            TempMetadataID elementID = createElementSymbol(tempGroup, symbol, false);
            
            groupID.addElement(elementID);
            
            return elementID;
        }
        return null;
    }
    
    /**
     * Get temporary group ID based on group name
     * @param tempGroup Group name
     * @return Metadata ID or null if not found
     */
    public TempMetadataID getTempGroupID(String tempGroup) {
        return tempGroups.get(tempGroup);    
    }
    
    /**
     * Get temporary element ID based on full element name
     * @param tempElement Element name
     * @return Metadata ID or null if not found
     */
    public TempMetadataID getTempElementID(String tempElement) {
        int index = tempElement.lastIndexOf(Symbol.SEPARATOR);
        if(index < 0) {
            return null;
        }
        String groupName = tempElement.substring(0, index);
            
        TempMetadataID groupID = tempGroups.get(groupName);
        if(groupID != null) {
        	for (TempMetadataID elementID : groupID.getElements()) {
                if(elementID.getID().equalsIgnoreCase(tempElement)) { 
                    return elementID;
                }
            }   
        }
        
        return null;
    }

    /**
     * Get temporary element ID based on group and element name parts
     * @param tempGroup Group name
     * @param tempElement Short element name
     * @return Metadata ID or null if not found
     */
    public List<TempMetadataID> getTempElementElementIDs(String tempGroup) {
        TempMetadataID groupID = getTempGroupID(tempGroup);       
        if(groupID != null) {
            return groupID.getElements();
        }
        
        return null;
    }
    
    public void addElementToTempGroup(String tempGroup, ElementSymbol symbol) {
        TempMetadataID groupID = tempGroups.get(tempGroup);        
        if(groupID != null) {
            groupID.addElement((TempMetadataID)symbol.getMetadataID());
        }
    }
    
    public TempMetadataID removeTempGroup(String tempGroup) {
        return tempGroups.remove(tempGroup);  
    }

}
