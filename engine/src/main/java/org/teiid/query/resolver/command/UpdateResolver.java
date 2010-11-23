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

package org.teiid.query.resolver.command;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.resolver.ProcedureContainerResolver;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.VariableResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.ProcedureReservedWords;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.SetClause;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;


/**
 * This class knows how to expand and resolve UDPATE commands.
 */
public class UpdateResolver extends ProcedureContainerResolver implements VariableResolver {

    /** 
     * @see org.teiid.query.resolver.ProcedureContainerResolver#resolveProceduralCommand(org.teiid.query.sql.lang.Command, org.teiid.query.metadata.TempMetadataAdapter, org.teiid.query.analysis.AnalysisRecord)
     */
    public void resolveProceduralCommand(Command command, TempMetadataAdapter metadata, AnalysisRecord analysis) 
        throws QueryMetadataException, QueryResolverException, TeiidComponentException {

        //Cast to known type
        Update update = (Update) command;

        // Resolve elements and functions
        Set<GroupSymbol> groups = new HashSet<GroupSymbol>();
        groups.add(update.getGroup());
        for (SetClause clause : update.getChangeList().getClauses()) {
        	ResolverVisitor.resolveLanguageObject(clause.getSymbol(), groups, null, metadata);
		}
        ResolverVisitor.resolveLanguageObject(update, groups, update.getExternalGroupContexts(), metadata);
        QueryResolver.resolveSubqueries(command, metadata, analysis);
    }
    
    /** 
     * @param metadata
     * @param group
     * @return
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     */
    protected String getPlan(QueryMetadataInterface metadata,
                           GroupSymbol group) throws TeiidComponentException,
                                             QueryMetadataException {
        return metadata.getUpdatePlan(group.getMetadataID());
    }

    /** 
     * @see org.teiid.query.resolver.VariableResolver#getVariableValues(org.teiid.query.sql.lang.Command, org.teiid.query.metadata.QueryMetadataInterface)
     */
    public Map getVariableValues(Command command,
                                 QueryMetadataInterface metadata) throws QueryMetadataException,
                                                                 QueryResolverException,
                                                                 TeiidComponentException {
        Map result = new HashMap();
        
        Update update = (Update) command;
        
        List updateVars = new LinkedList();
        
        for (Entry<ElementSymbol, Expression> entry : update.getChangeList().getClauseMap().entrySet()) {
        	ElementSymbol leftSymbol = entry.getKey();
            
            String varName = leftSymbol.getShortCanonicalName();
            String changingKey = ProcedureReservedWords.CHANGING + ElementSymbol.SEPARATOR + varName;
            String inputsKey = ProcedureReservedWords.INPUTS + ElementSymbol.SEPARATOR + varName;
            result.put(changingKey, new Constant(Boolean.TRUE));
            result.put(inputsKey, entry.getValue());
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
