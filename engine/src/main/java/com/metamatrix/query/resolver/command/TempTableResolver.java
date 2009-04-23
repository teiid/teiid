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
import java.util.HashSet;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.resolver.CommandResolver;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.resolver.util.ResolverVisitor;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Create;
import com.metamatrix.query.sql.lang.Drop;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.GroupSymbol;


/** 
 * @since 5.5
 */
public class TempTableResolver implements CommandResolver {

    /** 
     * @see com.metamatrix.query.resolver.CommandResolver#resolveCommand(com.metamatrix.query.sql.lang.Command, boolean, com.metamatrix.query.metadata.TempMetadataAdapter, com.metamatrix.query.analysis.AnalysisRecord, boolean)
     */
    public void resolveCommand(Command command, boolean useMetadataCommands, TempMetadataAdapter metadata, AnalysisRecord analysis, boolean resolveNullLiterals) 
        throws QueryMetadataException, QueryResolverException, MetaMatrixComponentException {
        
        if(command.getType() == Command.TYPE_CREATE) {
            Create create = (Create)command;
            GroupSymbol group = create.getTable();
            
            //assuming that all temp table creates are local, the user must use a local name
            if (group.getName().indexOf(ElementSymbol.SEPARATOR) != -1) {
                throw new QueryResolverException(QueryPlugin.Util.getString("TempTableResolver.unqualified_name_required", group.getName())); //$NON-NLS-1$
            }

            //this will only check non-temp groups
            Collection exitsingGroups = metadata.getMetadata().getGroupsForPartialName(group.getName());
            if(!exitsingGroups.isEmpty()) {
                throw new QueryResolverException(QueryPlugin.Util.getString("TempTableResolver.table_already_exists", group.getName())); //$NON-NLS-1$
            }
            //now we will be more specific for temp groups
            TempMetadataID id = metadata.getMetadataStore().getTempGroupID(group.getName());
            if (id != null && !metadata.isTemporaryTable(id)) {
                throw new QueryResolverException(QueryPlugin.Util.getString("TempTableResolver.table_already_exists", group.getName())); //$NON-NLS-1$        
            }
            //if we get here then either the group does not exist or has already been defined as a temp table
            //if it has been defined as a temp table, that's ok we'll use this as the new definition and throw an
            //exception at runtime if the user has not dropped the previous table yet
            ResolverUtil.addTempTable(metadata, group, create.getColumns());
            
            ResolverUtil.resolveGroup(((Create)command).getTable(), metadata);
            Set groups = new HashSet();
            groups.add(((Create)command).getTable());
            ResolverVisitor.resolveLanguageObject(command, groups, metadata);
        } else if(command.getType() == Command.TYPE_DROP) {
            ResolverUtil.resolveGroup(((Drop)command).getTable(), metadata);
        }
    }

}
