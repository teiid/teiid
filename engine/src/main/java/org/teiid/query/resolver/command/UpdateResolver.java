/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.resolver.command;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SQLConstants;
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

    @Override
    public Map<ElementSymbol, Expression> getVariableValues(Command command, boolean changingOnly,
                                 QueryMetadataInterface metadata) throws QueryMetadataException,
                                                                 TeiidComponentException {
        Map<ElementSymbol, Expression> result = new HashMap<ElementSymbol, Expression>();

        Update update = (Update) command;

        Map<ElementSymbol, Expression> changing = update.getChangeList().getClauseMap();

        for (Entry<ElementSymbol, Expression> entry : changing.entrySet()) {
            ElementSymbol leftSymbol = entry.getKey().clone();
            leftSymbol.getGroupSymbol().setName(ProcedureReservedWords.CHANGING);
            leftSymbol.setType(DataTypeManager.DefaultDataClasses.BOOLEAN);
            result.put(leftSymbol, new Constant(Boolean.TRUE));
            if (!changingOnly) {
                leftSymbol = entry.getKey().clone();
                leftSymbol.getGroupSymbol().setName(SQLConstants.Reserved.NEW);
                result.put(leftSymbol, entry.getValue());
            }
        }

        Collection<ElementSymbol> insertElmnts = ResolverUtil.resolveElementsInGroup(update.getGroup(), metadata);

        insertElmnts.removeAll(changing.keySet());

        Iterator<ElementSymbol> defaultIter = insertElmnts.iterator();
        while(defaultIter.hasNext()) {
            ElementSymbol varSymbol = defaultIter.next().clone();
            varSymbol.getGroupSymbol().setName(ProcedureReservedWords.CHANGING);
            varSymbol.setType(DataTypeManager.DefaultDataClasses.BOOLEAN);
            result.put(varSymbol, new Constant(Boolean.FALSE));
        }

        return result;
    }

}
