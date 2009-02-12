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
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.metadata.TempMetadataStore;
import com.metamatrix.query.resolver.ProcedureContainerResolver;
import com.metamatrix.query.resolver.util.ResolverVisitor;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Delete;
import com.metamatrix.query.sql.lang.GroupContext;
import com.metamatrix.query.sql.symbol.GroupSymbol;

/**
 * This class knows how to expand and resolve DELETE commands.
 */
public class DeleteResolver extends ProcedureContainerResolver {

    /** 
     * @see com.metamatrix.query.resolver.ProcedureContainerResolver#resolveProceduralCommand(com.metamatrix.query.sql.lang.Command, boolean, com.metamatrix.query.metadata.TempMetadataAdapter, com.metamatrix.query.analysis.AnalysisRecord)
     */
    public void resolveProceduralCommand(Command command, boolean useMetadataCommands, TempMetadataAdapter metadata, AnalysisRecord analysis) 
        throws QueryMetadataException, QueryResolverException, MetaMatrixComponentException {

        //Cast to known type
        Delete delete = (Delete) command;

        Set groups = new HashSet();
        groups.add(delete.getGroup());
        ResolverVisitor.resolveLanguageObject(delete, groups, delete.getExternalGroupContexts(), metadata);

    }
    
    /** 
     * @param metadata
     * @param group
     * @return
     * @throws MetaMatrixComponentException
     * @throws QueryMetadataException
     */
    protected String getPlan(QueryMetadataInterface metadata,
                           GroupSymbol group) throws MetaMatrixComponentException,
                                             QueryMetadataException {
        return metadata.getDeletePlan(group.getMetadataID());
    }
    
    @Override
    public GroupContext findChildCommandMetadata(Command command,
    		TempMetadataStore discoveredMetadata, boolean useMetadataCommands,
    		QueryMetadataInterface metadata) throws QueryMetadataException,
    		QueryResolverException, MetaMatrixComponentException {
    	super.findChildCommandMetadata(command, discoveredMetadata,
    			useMetadataCommands, metadata);
    	//defect 16451: don't expose input and changing variables to delete procedures
    	return null;
    }

}
