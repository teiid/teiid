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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.Schema;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.resolver.CommandResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Create;
import org.teiid.query.sql.lang.Drop;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;



/**
 * @since 5.5
 */
public class TempTableResolver implements CommandResolver {

    /**
     * @see org.teiid.query.resolver.CommandResolver#resolveCommand(org.teiid.query.sql.lang.Command, org.teiid.query.metadata.TempMetadataAdapter, boolean)
     */
    public void resolveCommand(Command command, TempMetadataAdapter metadata, boolean resolveNullLiterals)
        throws QueryMetadataException, QueryResolverException, TeiidComponentException {

        if(command.getType() == Command.TYPE_CREATE) {
            Create create = (Create)command;
            GroupSymbol group = create.getTable();

            //this will only check non-temp groups
            Collection exitsingGroups = metadata.getMetadata().getGroupsForPartialName(group.getName());
            if(!exitsingGroups.isEmpty()) {
                 throw new QueryResolverException(QueryPlugin.Event.TEIID30118, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30118, group.getName()));
            }
            if (metadata.getMetadata().hasProcedure(group.getName())) {
                 throw new QueryResolverException(QueryPlugin.Event.TEIID30118, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30118, group.getName()));
            }

            //now we will be more specific for temp groups
            TempMetadataID id = metadata.getMetadataStore().getTempGroupID(group.getName());
            if (id != null && !metadata.isTemporaryTable(id)) {
                 throw new QueryResolverException(QueryPlugin.Event.TEIID30118, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30118, group.getName()));
            }
            //if we get here then either the group does not exist or has already been defined as a temp table
            //if it has been defined as a temp table, that's ok we'll use this as the new definition and throw an
            //exception at runtime if the user has not dropped the previous table yet
            TempMetadataID tempTable = ResolverUtil.addTempTable(metadata, group, create.getColumnSymbols());
            ResolverUtil.resolveGroup(create.getTable(), metadata);
            if (create.getTable().getMetadataID() != tempTable) {
                //conflict with a fully qualified existing object
                throw new QueryResolverException(QueryPlugin.Event.TEIID30118, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30118, group.getName()));
            }
            Set<GroupSymbol> groups = new HashSet<GroupSymbol>();
            groups.add(create.getTable());
            ResolverVisitor.resolveLanguageObject(command, groups, metadata);
            addAdditionalMetadata(create, tempTable);
            tempTable.setOriginalMetadataID(create.getTableMetadata());
            if (create.getOn() != null) {
                Object mid = null;
                try {
                    mid = metadata.getModelID(create.getOn());
                } catch (QueryMetadataException e) {
                    throw new QueryResolverException(QueryPlugin.Event.TEIID31134, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31134, create.getOn()));
                }
                if (mid != null && (metadata.isVirtualModel(mid) || !(mid instanceof Schema))) {
                    throw new QueryResolverException(QueryPlugin.Event.TEIID31135, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31135, create.getOn()));
                }
                create.getTableMetadata().setParent((Schema)mid);
                tempTable.getTableData().setModel(mid);
            }
        } else if(command.getType() == Command.TYPE_DROP) {
            GroupSymbol table = ((Drop)command).getTable();
            ResolverUtil.resolveGroup(table, metadata);
            TempMetadataStore store = metadata.getMetadataStore();
            store.removeTempGroup(table.getName());
        }
    }

    public static void addAdditionalMetadata(Create create, TempMetadataID tempTable) {
        if (!create.getPrimaryKey().isEmpty()) {
            ArrayList<TempMetadataID> primaryKey = new ArrayList<TempMetadataID>(create.getPrimaryKey().size());
            for (ElementSymbol symbol : create.getPrimaryKey()) {
                Object mid = symbol.getMetadataID();
                if (mid instanceof TempMetadataID) {
                    primaryKey.add((TempMetadataID)mid);
                } else if (mid instanceof Column) {
                    //TODO: this breaks our normal metadata usage
                    primaryKey.add(tempTable.getElements().get(((Column)mid).getPosition() - 1));
                }
            }
            tempTable.setPrimaryKey(primaryKey);
        }
        for (int i = 0; i < create.getColumns().size(); i++) {
            Column column = create.getColumns().get(i);
            TempMetadataID tid = tempTable.getElements().get(i);
            if (column.isAutoIncremented()) {
                tid.setAutoIncrement(true);
            }
            if (column.getNullType() == NullType.No_Nulls) {
                tid.setNotNull(true);
            }
        }
    }

}
