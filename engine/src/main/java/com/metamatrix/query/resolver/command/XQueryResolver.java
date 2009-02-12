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

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.resolver.CommandResolver;
import com.metamatrix.query.sql.lang.Command;


/**
 * Resolves XQueries, which in metamatrix currently involves
 * taking the arguments to the doc() functions, adding them as
 * the sub commands of the XQuery, which are then themselves 
 * resolved by the Resolver framework.
 * 
 * XQuery resolution does not currently involve making sure the
 * XQuery's XPath statements referring to the virtual documents are
 * valid for those documents.
 */
public class XQueryResolver implements CommandResolver {

    /** 
     * @see com.metamatrix.query.resolver.CommandResolver#resolveCommand(com.metamatrix.query.sql.lang.Command, boolean, com.metamatrix.query.metadata.TempMetadataAdapter, com.metamatrix.query.analysis.AnalysisRecord, boolean)
     */
    public void resolveCommand(Command command, boolean useMetadataCommands, TempMetadataAdapter metadata, AnalysisRecord analysis, boolean resolveNullLiterals) 
        throws QueryMetadataException, QueryResolverException, MetaMatrixComponentException {
    }
}
