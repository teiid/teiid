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

package com.metamatrix.query.sql.util;

import java.util.*;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.query.metadata.*;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.QueryCommand;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.visitor.CommandCollectorVisitor;
import com.metamatrix.query.sql.visitor.ElementCollectorVisitor;
import com.metamatrix.query.sql.visitor.GroupCollectorVisitor;

/**
 * <p>The ElementSymbolOptimizer can be used to modify the appearance of the elements in a command.  The operations will
 * be performed on the command and all embedded subcommands, but not any further than that.  This
 * class should only be used on commands that have been resolved, as unresolved commands
 * do not contain enough metadata to determine the proper fully qualified or optimized 
 * element form.</p>
 */
public class ElementSymbolOptimizer {

    /**
     * Can't construct
     * @see java.lang.Object#Object()
     */
    private ElementSymbolOptimizer() {
    }

    /**
     * Get a command and all it's embedded commands recursively
     * @param command Command to start from
     * @param commandList Collected commands
     */
    private static void getExposedCommands(Command command, List commandList) {
        // Handle Unions
        if (command instanceof SetQuery){
            SetQuery setQuery = (SetQuery)command;
            for (QueryCommand setQueryCommand : setQuery.getQueryCommands()) {
                getExposedCommands(setQueryCommand, commandList);
            }   
        } else {
            commandList.add(command);

            List subCommands = CommandCollectorVisitor.getCommands(command, true);
            if(subCommands != null && subCommands.size() > 0) {
                for(int i=0; i<subCommands.size(); i++) {
                    Command subCommand = (Command) subCommands.get(i);
                    getExposedCommands(subCommand, commandList);
                }
            }
        }
    }

    /**
     * This method will convert all elements in a command to their fully qualified name.
     * @param command Command to convert
     */
    public static void fullyQualifyElements(Command command) {
        // Determine commands to fully qualify
        List commandsToQualify = new ArrayList();
        getExposedCommands(command, commandsToQualify);

        for(int i=0; i<commandsToQualify.size(); i++) {
            Command currentCommand = (Command) commandsToQualify.get(i);
            Collection elements = ElementCollectorVisitor.getElements(currentCommand, false);
            Iterator elementIter = elements.iterator();
            while(elementIter.hasNext()) {
                ElementSymbol element = (ElementSymbol) elementIter.next();

                fullyQualifyElement(element);
            }
        }
    }

    /**
     * Method fullyQualifyElement.
     * @param element
     */
    private static void fullyQualifyElement(ElementSymbol element) {
        element.setDisplayFullyQualified(true);
    }


    
    /**    
     * This method will convert all elements in a command to their shortest possible unambiguous name.
     * @param command Command to convert
     */
    public static void optimizeElements(Command command, QueryMetadataInterface metadata)
    throws QueryMetadataException, MetaMatrixComponentException{
                
        // Determine commands to optimize        
        List commandsToOptimize = new ArrayList();
        getExposedCommands(command, commandsToOptimize);
        
        for(int i=0; i<commandsToOptimize.size(); i++) {
            Command currentCommand = (Command) commandsToOptimize.get(i);         
            TempMetadataAdapter facade = new TempMetadataAdapter(metadata, new TempMetadataStore(currentCommand.getTemporaryMetadata()));
            Collection externalGroups = currentCommand.getAllExternalGroups();
            Collection groups = GroupCollectorVisitor.getGroups(currentCommand, false);             
            
            optimizeElements(currentCommand, groups, externalGroups, facade);
        }
    }

    private static boolean isXMLCommand(Command command, QueryMetadataInterface metadata)
    throws QueryMetadataException, MetaMatrixComponentException{
        // Check groups
        Collection groups = GroupCollectorVisitor.getGroups(command, true);
        if(groups.size() != 1) {
            return false;
        }

        // Check group symbol 
        GroupSymbol group = (GroupSymbol) groups.iterator().next();

        // check if it is an XML group
        if (metadata.isXMLGroup(group.getMetadataID())){
            return true;
        }
        
        return false;
    } 
    
    /**
     * Method optimizeElements.
     * @param command
     * @param elements
     * @param groups
     * @param externalGroups
     * @param facade
     */
    private static void optimizeElements(
        Command command,
        Collection groups,
        Collection externalGroups,
        QueryMetadataInterface metadata) 
    throws QueryMetadataException, MetaMatrixComponentException {
            
        switch(command.getType()) {
            case Command.TYPE_INSERT:
            case Command.TYPE_UPDATE:
            case Command.TYPE_DELETE:
//                optimizeUpdateCommand(command, groups, externalGroups, metadata);
            case Command.TYPE_STORED_PROCEDURE:
//                optimizeStoredProcedure(command, groups, externalGroups, metadata);
            case Command.TYPE_QUERY:
                // check for XML
                optimizeCommand(command, groups, externalGroups, metadata);                
                break;
                               
            case Command.TYPE_UPDATE_PROCEDURE:
                // for now do nothing
                break;            
        }
    }

    /**
     * Method optimizeCommand.  XML Commands are not currently optimized.
     * @param command
     * @param groups
     * @param externalGroups
     * @param metadata
     */
    private static void optimizeCommand(
        Command command,
        Collection groups,
        Collection externalGroups,
        QueryMetadataInterface metadata)
        throws QueryMetadataException, MetaMatrixComponentException {
            
        if (isXMLCommand(command, metadata)){
            return;
        }
        
        final boolean REMOVE_DUPLICATES = false;
        Collection elements = ElementCollectorVisitor.getElements(command, REMOVE_DUPLICATES);
        Map shortNameMap = mapShortNamesToGroups(groups, externalGroups, metadata);
        
        Iterator i = elements.iterator();
        while (i.hasNext()) {
            ElementSymbol element = (ElementSymbol)i.next();
            String elementFullName = metadata.getFullName(element.getMetadataID());
            String shortNameKey = metadata.getShortElementName(elementFullName).toUpperCase();
            Set groupSet = (Set)shortNameMap.get(shortNameKey);
            if (groupSet != null && groupSet.size() <= 1){
                element.setDisplayFullyQualified(false);
            }
        }            
    }
    
    /**
     * Return a Map of String element (or parameter) short names to 
     * Set of GroupSymbols that have an element with that name
     * @param groups from Command
     * @param externalGroups from Command
     * @param metadata
     * @return Map of String element (or parameter) short names to 
     * Set of GroupSymbols that have an element with that name
     */
    private static Map mapShortNamesToGroups(Collection groups, Collection externalGroups, QueryMetadataInterface metadata) 
    throws QueryMetadataException, MetaMatrixComponentException {

        Map result = new HashMap();
        Collection allGroups = new ArrayList(groups);
        allGroups.addAll(externalGroups);

        Iterator i = allGroups.iterator();
        while (i.hasNext()) {
            GroupSymbol group = (GroupSymbol)i.next();
            Iterator elemIDs = metadata.getElementIDsInGroupID(group.getMetadataID()).iterator();
            while (elemIDs.hasNext()) {
                ElementSymbol element = new ElementSymbol(metadata.getFullName(elemIDs.next()));
                String shortNameKey = element.getShortName().toUpperCase();
                Set groupSet = (Set)result.get(shortNameKey);
                if (groupSet == null){
                    groupSet = new HashSet();
                    result.put(shortNameKey, groupSet);
                }
                groupSet.add(group);
            }
        }
        
        return result;
    }
}
