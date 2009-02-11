/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.query.processor.dynamic;

import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.BufferManager.TupleSourceType;
import com.metamatrix.core.id.IDGenerator;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.QueryOptimizer;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.QueryProcessor;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.TypeRetrievalUtil;


public class SimpleQueryProcessorFactory implements QueryProcessor.ProcessorFactory {

	private QueryMetadataInterface metadata;
	private CapabilitiesFinder finder;
	private IDGenerator idGenerator;
	private BufferManager bufferMgr;
	private ProcessorDataManager dataMgr;
	
	public SimpleQueryProcessorFactory(BufferManager bufferMgr,
			ProcessorDataManager dataMgr, CapabilitiesFinder finder,
			IDGenerator idGenerator, QueryMetadataInterface metadata) {
		this.bufferMgr = bufferMgr;
		this.dataMgr = dataMgr;
		this.finder = finder;
		this.idGenerator = idGenerator;
		this.metadata = metadata;
	}

	@Override
	public QueryProcessor createQueryProcessor(String sql, String recursionGroup, CommandContext commandContext)
			throws MetaMatrixProcessingException, MetaMatrixComponentException {
		Command command = QueryParser.getQueryParser().parseCommand(sql);
		QueryResolver.resolveCommand(command, metadata);
		QueryRewriter.rewrite(command, null, metadata, commandContext);
		ProcessorPlan plan = QueryOptimizer.optimizePlan(command, metadata,
				idGenerator, finder, AnalysisRecord.createNonRecordingRecord(),
				commandContext);

		List elements = plan.getOutputElements();
		CommandContext copy = (CommandContext) commandContext.clone();
		TupleSourceID resultsId = bufferMgr.createTupleSource(elements,
				TypeRetrievalUtil.getTypeNames(elements), commandContext
						.getConnectionID(), TupleSourceType.PROCESSOR);
		copy.setTupleSourceID(resultsId);

		return new QueryProcessor(plan, copy, bufferMgr, dataMgr);
	}
}
