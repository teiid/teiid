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

package com.metamatrix.query.resolver.command;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.resolver.ProcedureContainerResolver;
import com.metamatrix.query.resolver.VariableResolver;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.resolver.util.ResolverVisitor;
import com.metamatrix.query.sql.ProcedureReservedWords;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Update;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.GroupSymbol;

/**
 * This class knows how to expand and resolve UDPATE commands.
 */
public class UpdateResolver extends ProcedureContainerResolver implements VariableResolver {

    /** 
     * @see com.metamatrix.query.resolver.ProcedureContainerResolver#resolveProceduralCommand(com.metamatrix.query.sql.lang.Command, boolean, com.metamatrix.query.metadata.TempMetadataAdapter, com.metamatrix.query.analysis.AnalysisRecord)
     */
    public void resolveProceduralCommand(Command command, boolean useMetadataCommands, TempMetadataAdapter metadata, AnalysisRecord analysis) 
        throws QueryMetadataException, QueryResolverException, MetaMatrixComponentException {

        //Cast to known type
        Update update = (Update) command;

        // Resolve elements and functions
        Set<GroupSymbol> groups = new HashSet<GroupSymbol>();
        groups.add(update.getGroup());
        ResolverVisitor.resolveLanguageObject(update, groups, update.getExternalGroupContexts(), metadata);
    }
    
    /** 
     * @param metadata
     * @param group
     * @return
     * @throws MetaMatrixComponentException
     * @throws QueryMetadataException
     */
    protected String getPlan(QueryMetadataInterface metadata,
                           GroupSymbol group) throws MetaMatrixComponentException,
                                             QueryMetadataException {
        return metadata.getUpdatePlan(group.getMetadataID());
    }

    /** 
     * @see com.metamatrix.query.resolver.VariableResolver#getVariableValues(com.metamatrix.query.sql.lang.Command, com.metamatrix.query.metadata.QueryMetadataInterface)
     */
    public Map getVariableValues(Command command,
                                 QueryMetadataInterface metadata) throws QueryMetadataException,
                                                                 QueryResolverException,
                                                                 MetaMatrixComponentException {
        Map result = new HashMap();
        
        Update update = (Update) command;
        
        List updateVars = new LinkedList();
        
        for (Entry<ElementSymbol, Expression> entry : update.getChangeList().getClauseMap().entrySet()) {
        	ElementSymbol leftSymbol = entry.getKey();
            
            String varName = leftSymbol.getShortCanonicalName();
            String changingKey = ProcedureReservedWords.CHANGING + ElementSymbol.SEPARATOR + varName;
            String inputKey = ProcedureReservedWords.INPUT + ElementSymbol.SEPARATOR + varName;
            
            result.put(changingKey, new Constant(Boolean.TRUE));
            result.put(inputKey, entry.getValue());
            
            updateVars.add(leftSymbol);
        }
        
        Collection insertElmnts = ResolverUtil.resolveElementsInGroup(update.getGroup(), metadata);

        insertElmnts.removeAll(updateVars);

        Iterator defaultIter = insertElmnts.iterator();
        while(defaultIter.hasNext()) {
            ElementSymbol varSymbol = (ElementSymbol) defaultIter.next();

            String varName = varSymbol.getShortCanonicalName();
            String changingKey = ProcedureReservedWords.CHANGING + ElementSymbol.SEPARATOR + varName;
            
            result.put(changingKey, new Constant(Boolean.FALSE));
        }
        
        return result;
    }

}
