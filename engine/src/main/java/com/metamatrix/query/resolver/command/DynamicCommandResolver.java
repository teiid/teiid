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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.resolver.CommandResolver;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.resolver.util.ResolverVisitor;
import com.metamatrix.query.sql.ProcedureReservedWords;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.DynamicCommand;
import com.metamatrix.query.sql.lang.SetClause;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;

public class DynamicCommandResolver implements CommandResolver {

    /** 
     * @see com.metamatrix.query.resolver.CommandResolver#resolveCommand(com.metamatrix.query.sql.lang.Command, boolean, TempMetadataAdapter, AnalysisRecord, boolean)
     */
    public void resolveCommand(Command command, boolean useMetadataCommands, TempMetadataAdapter metadata, AnalysisRecord analysis, boolean resolveNullLiterals) 
        throws QueryMetadataException, QueryResolverException, MetaMatrixComponentException {

        DynamicCommand dynamicCmd = (DynamicCommand)command;
        
        Iterator columns = dynamicCmd.getAsColumns().iterator();

        Set groups = new HashSet();
        
        //if there is no into group, just create temp metadata ids
        if (dynamicCmd.getIntoGroup() == null) {
            while (columns.hasNext()) {
                ElementSymbol column = (ElementSymbol)columns.next();
                column.setMetadataID(new TempMetadataID(column.getShortCanonicalName(), column.getType()));
            }
        } else if (dynamicCmd.getIntoGroup().isTempGroupSymbol()) {
            while (columns.hasNext()) {
                ElementSymbol column = (ElementSymbol)columns.next();
                column.setName(dynamicCmd.getIntoGroup().getCanonicalName() + SingleElementSymbol.SEPARATOR + column.getShortName());
            }
        }
        
        ResolverVisitor.resolveLanguageObject(dynamicCmd, groups, dynamicCmd.getExternalGroupContexts(), metadata);
        
        String sqlType = DataTypeManager.getDataTypeName(dynamicCmd.getSql().getType());
        String targetType = DataTypeManager.DefaultDataTypes.STRING;
        
        if (!targetType.equals(sqlType) && !DataTypeManager.isImplicitConversion(sqlType, targetType)) {
            throw new QueryResolverException(QueryPlugin.Util.getString("DynamicCommandResolver.SQL_String", sqlType)); //$NON-NLS-1$
        }
        
        if (dynamicCmd.getUsing() != null && !dynamicCmd.getUsing().isEmpty()) {
            for (SetClause clause : dynamicCmd.getUsing().getClauses()) {
                ElementSymbol id = clause.getSymbol();
                id.setName(ProcedureReservedWords.USING + SingleElementSymbol.SEPARATOR + id.getShortName());
                id.setGroupSymbol(new GroupSymbol(ProcedureReservedWords.USING));
                id.setType(clause.getValue().getType());
                id.setMetadataID(new TempMetadataID(id.getCanonicalName(), id.getType()));
            }
        }
        
        GroupSymbol intoSymbol = dynamicCmd.getIntoGroup();
        if (intoSymbol != null) {
            if (!intoSymbol.isImplicitTempGroupSymbol()) {
                ResolverUtil.resolveGroup(intoSymbol, metadata);
            } else {
                List symbols = dynamicCmd.getAsColumns();
                ResolverUtil.resolveImplicitTempGroup(metadata, intoSymbol, symbols);
            }
        }
    }
}
