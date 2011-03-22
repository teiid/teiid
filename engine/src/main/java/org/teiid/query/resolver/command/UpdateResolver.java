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
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
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
     * @see org.teiid.query.resolver.ProcedureContainerResolver#resolveProceduralCommand(org.teiid.query.sql.lang.Command, org.teiid.query.metadata.TempMetadataAdapter)
     */
    public void resolveProceduralCommand(Command command, TempMetadataAdapter metadata) 
        throws QueryMetadataException, QueryResolverException, TeiidComponentException {

        //Cast to known type
        Update update = (Update) command;

        // Resolve elements and functions
        Set<GroupSymbol> groups = new HashSet<GroupSymbol>();
        groups.add(update.getGroup());
        for (SetClause clause : update.getChangeList().getClauses()) {
        	ResolverVisitor.resolveLanguageObject(clause.getSymbol(), groups, null, metadata);
		}
        QueryResolver.resolveSubqueries(command, metadata, groups);
        ResolverVisitor.resolveLanguageObject(update, groups, update.getExternalGroupContexts(), metadata);
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
    public Map<ElementSymbol, Expression> getVariableValues(Command command, boolean changingOnly,
                                 QueryMetadataInterface metadata) throws QueryMetadataException,
                                                                 TeiidComponentException {
        Map<ElementSymbol, Expression> result = new HashMap<ElementSymbol, Expression>();
        
        Update update = (Update) command;
        
        Map<ElementSymbol, Expression> changing = update.getChangeList().getClauseMap();
        
        for (Entry<ElementSymbol, Expression> entry : changing.entrySet()) {
        	ElementSymbol leftSymbol = entry.getKey().clone();
            leftSymbol.getGroupSymbol().setName(ProcedureReservedWords.CHANGING);
            result.put(leftSymbol, new Constant(Boolean.TRUE));
            if (!changingOnly) {
            	leftSymbol = leftSymbol.clone();
            	leftSymbol.getGroupSymbol().setName(ProcedureReservedWords.INPUTS);
            	result.put(leftSymbol, entry.getValue());
            }
        }
        
        Collection<ElementSymbol> insertElmnts = ResolverUtil.resolveElementsInGroup(update.getGroup(), metadata);

        insertElmnts.removeAll(changing.keySet());

        Iterator<ElementSymbol> defaultIter = insertElmnts.iterator();
        while(defaultIter.hasNext()) {
            ElementSymbol varSymbol = defaultIter.next().clone();
            varSymbol.getGroupSymbol().setName(ProcedureReservedWords.CHANGING);
            result.put(varSymbol, new Constant(Boolean.FALSE));
        }
        
        return result;
    }

}
