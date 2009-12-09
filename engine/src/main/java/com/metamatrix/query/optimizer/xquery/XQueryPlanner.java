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

package com.metamatrix.query.optimizer.xquery;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.core.id.IDGenerator;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.CommandPlanner;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.xquery.XQueryPlan;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.XQuery;
import com.metamatrix.query.util.CommandContext;

/**
 * Planner for XQuery execution plans
 */
public class XQueryPlanner implements CommandPlanner {

    /**
     * @see com.metamatrix.query.optimizer.CommandPlanner#optimize(Command, com.metamatrix.core.id.IDGenerator, com.metamatrix.query.metadata.QueryMetadataInterface, com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder, com.metamatrix.query.analysis.AnalysisRecord, CommandContext)
     */
    public ProcessorPlan optimize(Command command, IDGenerator idGenerator, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, AnalysisRecord analysisRecord, CommandContext context)
        throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {

    	return new XQueryPlan((XQuery)command);        
    }

}
