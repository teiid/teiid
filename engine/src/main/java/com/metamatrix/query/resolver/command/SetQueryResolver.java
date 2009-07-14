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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.resolver.CommandResolver;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.QueryCommand;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.util.ErrorMessageKeys;

public class SetQueryResolver implements CommandResolver {

    /**
     * @see com.metamatrix.query.resolver.CommandResolver#resolveCommand(com.metamatrix.query.sql.lang.Command, java.util.Collection, TempMetadataAdapter, AnalysisRecord, boolean)
     */
    public void resolveCommand(Command command, boolean useMetadataCommands, TempMetadataAdapter metadata, AnalysisRecord analysis, boolean resolveNullLiterals)
        throws QueryMetadataException, QueryResolverException, MetaMatrixComponentException {

        SetQuery setQuery = (SetQuery) command;
        
        QueryCommand firstCommand = setQuery.getLeftQuery();
        
        QueryResolver.setChildMetadata(firstCommand, setQuery);
        QueryResolver.resolveCommand(firstCommand, Collections.EMPTY_MAP, useMetadataCommands, metadata.getMetadata(), analysis, false);

        List firstProject = firstCommand.getProjectedSymbols();
        List<Class<?>> firstProjectTypes = new ArrayList<Class<?>>();
        for (Iterator j = firstProject.iterator(); j.hasNext();) {
            SingleElementSymbol symbol = (SingleElementSymbol)j.next();
            firstProjectTypes.add(symbol.getType());
        }

        QueryCommand rightCommand = setQuery.getRightQuery();
        
        QueryResolver.setChildMetadata(rightCommand, setQuery);
        QueryResolver.resolveCommand(rightCommand, Collections.EMPTY_MAP, useMetadataCommands, metadata.getMetadata(), analysis, false);

        if (firstProject.size() != rightCommand.getProjectedSymbols().size()) {
            throw new QueryResolverException(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0035, setQuery.getOperation()));
        }
        checkSymbolTypes(firstProjectTypes, rightCommand.getProjectedSymbols());
        
        if (resolveNullLiterals) {
            for (int i = 0; i < firstProjectTypes.size(); i++) {
                Class<?> clazz = (Class<?>) firstProjectTypes.get(i);
                
                if (clazz == null || clazz.equals(DataTypeManager.DefaultDataClasses.NULL)) {
                    firstProjectTypes.set(i, DataTypeManager.DefaultDataClasses.STRING);
                }
            }
        }

        setQuery.setProjectedTypes(firstProjectTypes);
        
        // ORDER BY clause
        if(setQuery.getOrderBy() != null) {
            List validGroups = Collections.EMPTY_LIST;
            //order by elements must use the short name of the projected symbols
            ResolverUtil.resolveOrderBy(setQuery.getOrderBy(), validGroups, setQuery.getProjectedSymbols(), metadata, false);
        } 

        setProjectedTypes(setQuery, firstProjectTypes);
        
        if (setQuery.getLimit() != null) {
            ResolverUtil.resolveLimit(setQuery.getLimit());
        }
        
        setQuery.setTemporaryMetadata(new HashMap(firstCommand.getTemporaryMetadata()));
    }

    private void setProjectedTypes(SetQuery setQuery,
                                   List firstProjectTypes) throws QueryResolverException {
        for (QueryCommand subCommand : setQuery.getQueryCommands()) {
            if (!(subCommand instanceof SetQuery)) {
                continue;
            }
            SetQuery child = (SetQuery)subCommand;
            List projectedSymbols = child.getProjectedSymbols();
            if (child.getOrderBy() != null) {
                for (int j = 0; j < projectedSymbols.size(); j++) {
                    SingleElementSymbol ses = (SingleElementSymbol)projectedSymbols.get(j);
                    Class targetType = (Class)firstProjectTypes.get(j);
                    if (ses.getType() != targetType && ResolverUtil.orderByContainsVariable(child.getOrderBy(), ses, j)) {
                        String sourceTypeName = DataTypeManager.getDataTypeName(ses.getType());
                        String targetTypeName = DataTypeManager.getDataTypeName(targetType);
                        throw new QueryResolverException(QueryPlugin.Util.getString("UnionQueryResolver.type_conversion", //$NON-NLS-1$
                                                                                    new Object[] {ses, sourceTypeName, targetTypeName}));
                    }
                }
            }
            child.setProjectedTypes(firstProjectTypes);
            setProjectedTypes(child, firstProjectTypes);
        }
    }
    
	static void checkSymbolTypes(List firstProjectTypes, List projSymbols) {
        for(int j=0; j<projSymbols.size(); j++){
            Class firstProjType = (Class)firstProjectTypes.get(j);
    		SingleElementSymbol projSymbol = (SingleElementSymbol)projSymbols.get(j);
            Class projType = projSymbol.getType();
            
            if(firstProjType.equals(projType)){
                continue;
            }
            
            String sourceType = DataTypeManager.getDataTypeName(firstProjType);
            String targetType = DataTypeManager.getDataTypeName(projType);
            
            String commonType = ResolverUtil.getCommonType(new String[] {sourceType, targetType});
            
            if (commonType == null) {
            	commonType = DataTypeManager.DefaultDataTypes.OBJECT;
            }
            
            firstProjectTypes.set(j, DataTypeManager.getDataTypeClass(commonType));
        }
	}
}
