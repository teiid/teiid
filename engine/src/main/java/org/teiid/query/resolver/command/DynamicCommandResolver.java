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
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.resolver.CommandResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.ProcedureReservedWords;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.DynamicCommand;
import org.teiid.query.sql.lang.SetClause;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;


public class DynamicCommandResolver implements CommandResolver {

    /**
     * @see org.teiid.query.resolver.CommandResolver#resolveCommand(org.teiid.query.sql.lang.Command, TempMetadataAdapter, boolean)
     */
    public void resolveCommand(Command command, TempMetadataAdapter metadata, boolean resolveNullLiterals)
        throws QueryMetadataException, QueryResolverException, TeiidComponentException {

        DynamicCommand dynamicCmd = (DynamicCommand)command;

        Iterator columns = dynamicCmd.getAsColumns().iterator();

        Set groups = new HashSet();
        boolean resolvedColumns = false;

        //if there is no into group, just create temp metadata ids
        if (dynamicCmd.getIntoGroup() == null) {
            while (columns.hasNext()) {
                ElementSymbol column = (ElementSymbol)columns.next();
                column.setMetadataID(new TempMetadataID(column.getShortName(), column.getType()));
            }
        } else if (dynamicCmd.getIntoGroup().isTempGroupSymbol()) {
            resolvedColumns = true;
            while (columns.hasNext()) {
                ElementSymbol column = (ElementSymbol)columns.next();
                column.setGroupSymbol(new GroupSymbol(dynamicCmd.getIntoGroup().getName()));
            }
        }

        ResolverVisitor.resolveLanguageObject(dynamicCmd, groups, dynamicCmd.getExternalGroupContexts(), metadata);

        String sqlType = DataTypeManager.getDataTypeName(dynamicCmd.getSql().getType());
        String targetType = DataTypeManager.DefaultDataTypes.CLOB;

        if (!targetType.equals(sqlType) && !DataTypeManager.isImplicitConversion(sqlType, targetType)) {
             throw new QueryResolverException(QueryPlugin.Event.TEIID30100, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30100, sqlType));
        }
        dynamicCmd.setSql(ResolverUtil.convertExpression(dynamicCmd.getSql(), targetType, metadata));

        if (dynamicCmd.getUsing() != null && !dynamicCmd.getUsing().isEmpty()) {
            for (SetClause clause : dynamicCmd.getUsing().getClauses()) {
                ElementSymbol id = clause.getSymbol();
                id.setGroupSymbol(new GroupSymbol(ProcedureReservedWords.DVARS));
                id.setType(clause.getValue().getType());
                id.setMetadataID(new TempMetadataID(id.getName(), id.getType()));
            }
        }

        GroupSymbol intoSymbol = dynamicCmd.getIntoGroup();
        if (intoSymbol != null) {
            if (!intoSymbol.isImplicitTempGroupSymbol()) {
                ResolverUtil.resolveGroup(intoSymbol, metadata);
                if (!resolvedColumns) {
                    //must be a temp table from a higher scope
                    for (ElementSymbol column : (List<ElementSymbol>)dynamicCmd.getAsColumns()) {
                        column.setGroupSymbol(dynamicCmd.getIntoGroup().clone());
                        //if we want the insert to happen based upon column name matching we need to resolve
                        //currently we expect to match positionally
                        //ResolverVisitor.resolveLanguageObject(column, metadata);
                    }
                }
            } else {
                List symbols = dynamicCmd.getAsColumns();
                ResolverUtil.resolveImplicitTempGroup(metadata, intoSymbol, symbols);
            }
        }
    }
}
