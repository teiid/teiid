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
import java.util.List;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.resolver.CommandResolver;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.symbol.Expression;


public class SetQueryResolver implements CommandResolver {

    /**
     * @see org.teiid.query.resolver.CommandResolver#resolveCommand(org.teiid.query.sql.lang.Command, TempMetadataAdapter, boolean)
     */
    public void resolveCommand(Command command, TempMetadataAdapter metadata, boolean resolveNullLiterals)
        throws QueryMetadataException, QueryResolverException, TeiidComponentException {

        SetQuery setQuery = (SetQuery) command;

        SimpleQueryResolver.resolveWith(metadata, setQuery);

        QueryCommand firstCommand = setQuery.getLeftQuery();

        QueryResolver.setChildMetadata(firstCommand, setQuery);
        QueryResolver.resolveCommand(firstCommand, metadata.getMetadata(), false);

        QueryCommand rightCommand = setQuery.getRightQuery();

        QueryResolver.setChildMetadata(rightCommand, setQuery);
        QueryResolver.resolveCommand(rightCommand, metadata.getMetadata(), false);

        resolveSetQuery(metadata, resolveNullLiterals, setQuery, firstCommand,
                rightCommand);
    }

    public void resolveSetQuery(TempMetadataAdapter metadata,
            boolean resolveNullLiterals, SetQuery setQuery,
            QueryCommand firstCommand, QueryCommand rightCommand)
            throws QueryResolverException, QueryMetadataException,
            TeiidComponentException {
        List<Expression> firstProject = firstCommand.getProjectedSymbols();
        List<Class<?>> firstProjectTypes = new ArrayList<Class<?>>();
        for (Expression symbol : firstProject) {
            firstProjectTypes.add(symbol.getType());
        }

        if (firstProject.size() != rightCommand.getProjectedSymbols().size()) {
             throw new QueryResolverException(QueryPlugin.Event.TEIID30147, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30147, setQuery.getOperation()));
        }
        checkSymbolTypes(firstProjectTypes, rightCommand.getProjectedSymbols());

        if (resolveNullLiterals) {
            for (int i = 0; i < firstProjectTypes.size(); i++) {
                Class<?> clazz = firstProjectTypes.get(i);

                if (clazz == null || clazz.equals(DataTypeManager.DefaultDataClasses.NULL)) {
                    firstProjectTypes.set(i, DataTypeManager.DefaultDataClasses.STRING);
                }
            }
        }

        setQuery.setProjectedTypes(firstProjectTypes, metadata.getMetadata());

        // ORDER BY clause
        if(setQuery.getOrderBy() != null) {
            //order by elements must use the short name of the projected symbols
            ResolverUtil.resolveOrderBy(setQuery.getOrderBy(), setQuery, metadata);
        }

        setProjectedTypes(setQuery, firstProjectTypes, metadata.getMetadata());

        if (setQuery.getLimit() != null) {
            ResolverUtil.resolveLimit(setQuery.getLimit());
        }

        setQuery.setTemporaryMetadata(firstCommand.getTemporaryMetadata().clone());
    }

    private void setProjectedTypes(SetQuery setQuery,
                                   List<Class<?>> firstProjectTypes, QueryMetadataInterface metadata) throws QueryResolverException {
        for (QueryCommand subCommand : setQuery.getQueryCommands()) {
            if (!(subCommand instanceof SetQuery)) {
                continue;
            }
            SetQuery child = (SetQuery)subCommand;
            List projectedSymbols = child.getProjectedSymbols();
            if (child.getOrderBy() != null) {
                for (int j = 0; j < projectedSymbols.size(); j++) {
                    Expression ses = (Expression)projectedSymbols.get(j);
                    Class<?> targetType = firstProjectTypes.get(j);
                    if (ses.getType() != targetType && orderByContainsVariable(child.getOrderBy(), ses, j)) {
                        String sourceTypeName = DataTypeManager.getDataTypeName(ses.getType());
                        String targetTypeName = DataTypeManager.getDataTypeName(targetType);
                        throw new QueryResolverException(QueryPlugin.Util.getString("UnionQueryResolver.type_conversion", //$NON-NLS-1$
                                                                                    new Object[] {ses, sourceTypeName, targetTypeName}));
                    }
                }
            }
            child.setProjectedTypes(firstProjectTypes, metadata);
            setProjectedTypes(child, firstProjectTypes, metadata);
        }
    }

    /**
     * Checks if a variable is in the ORDER BY
     * @param position 0-based index of the variable
     * @return True if the ORDER BY contains the element
     */
    public static boolean orderByContainsVariable(OrderBy orderBy, Expression ses, int position) {
        for (OrderByItem item : orderBy.getOrderByItems()) {
            if (item.getExpressionPosition() == position) {
                return true;
            }
        }
        return false;
    }

    static void checkSymbolTypes(List<Class<?>> firstProjectTypes, List<Expression> projSymbols) throws QueryResolverException {
        for(int j=0; j<projSymbols.size(); j++){
            Class<?> firstProjType = firstProjectTypes.get(j);
            Expression projSymbol = projSymbols.get(j);
            Class<?> projType = projSymbol.getType();

            if (firstProjType == null) {
                if (projType != null) {
                    firstProjectTypes.set(j, projType);
                }
                continue;
            }

            if(firstProjType.equals(projType)){
                continue;
            }

            String sourceType = DataTypeManager.getDataTypeName(firstProjType);
            String targetType = DataTypeManager.getDataTypeName(projType);

            String commonType = ResolverUtil.getCommonRuntimeType(new String[] {sourceType, targetType});

            if (commonType == null) {
                commonType = DataTypeManager.DefaultDataTypes.OBJECT;
            }

            firstProjectTypes.set(j, DataTypeManager.getDataTypeClass(commonType));
        }
    }
}
