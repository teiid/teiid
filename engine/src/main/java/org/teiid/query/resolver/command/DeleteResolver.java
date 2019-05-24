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

import java.util.HashSet;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.resolver.ProcedureContainerResolver;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.symbol.GroupSymbol;


/**
 * This class knows how to expand and resolve DELETE commands.
 */
public class DeleteResolver extends ProcedureContainerResolver {

    /**
     * @see org.teiid.query.resolver.ProcedureContainerResolver#resolveProceduralCommand(org.teiid.query.sql.lang.Command, org.teiid.query.metadata.TempMetadataAdapter)
     */
    public void resolveProceduralCommand(Command command, TempMetadataAdapter metadata)
        throws QueryMetadataException, QueryResolverException, TeiidComponentException {

        //Cast to known type
        Delete delete = (Delete) command;

        Set<GroupSymbol> groups = new HashSet<GroupSymbol>();
        groups.add(delete.getGroup());
        QueryResolver.resolveSubqueries(command, metadata, groups);
        ResolverVisitor.resolveLanguageObject(delete, groups, delete.getExternalGroupContexts(), metadata);
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
        return metadata.getDeletePlan(group.getMetadataID());
    }

}
