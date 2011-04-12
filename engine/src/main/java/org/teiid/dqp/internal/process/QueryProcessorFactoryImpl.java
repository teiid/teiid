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

package org.teiid.dqp.internal.process;

import java.util.Arrays;
import java.util.List;

import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.id.IDGenerator;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.QueryOptimizer;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.parser.ParseInfo;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.sql.visitor.ReferenceCollectorVisitor;
import org.teiid.query.util.CommandContext;
import org.teiid.query.validator.AbstractValidationVisitor;
import org.teiid.query.validator.ValidationVisitor;

public class QueryProcessorFactoryImpl implements QueryProcessor.ProcessorFactory {

	private QueryMetadataInterface metadata;
	private CapabilitiesFinder finder;
	private IDGenerator idGenerator;
	private BufferManager bufferMgr;
	private ProcessorDataManager dataMgr;
	
	public QueryProcessorFactoryImpl(BufferManager bufferMgr,
			ProcessorDataManager dataMgr, CapabilitiesFinder finder,
			IDGenerator idGenerator, QueryMetadataInterface metadata) {
		this.bufferMgr = bufferMgr;
		this.dataMgr = dataMgr;
		this.finder = finder;
		this.idGenerator = idGenerator;
		this.metadata = metadata;
	}

	@Override
	public QueryProcessor createQueryProcessor(String query, String recursionGroup, CommandContext commandContext, Object... params) throws TeiidProcessingException, TeiidComponentException {
		PreparedPlan pp = commandContext.getPlan(query);
        CommandContext copy = commandContext.clone();
        if (recursionGroup != null) {
        	copy.pushCall(recursionGroup);
        }
		if (pp == null) {
			ParseInfo parseInfo = new ParseInfo();
			Command newCommand = QueryParser.getQueryParser().parseCommand(query, parseInfo);
	        QueryResolver.resolveCommand(newCommand, metadata);            
	        
	        List<Reference> references = ReferenceCollectorVisitor.getReferences(newCommand);
	        
	        AbstractValidationVisitor visitor = new ValidationVisitor();
	        Request.validateWithVisitor(visitor, metadata, newCommand);
	        Determinism determinismLevel = copy.resetDeterminismLevel();
	        newCommand = QueryRewriter.rewrite(newCommand, metadata, copy);
	        AnalysisRecord record = new AnalysisRecord(false, false);
	        ProcessorPlan plan = QueryOptimizer.optimizePlan(newCommand, metadata, idGenerator, finder, record, copy);
	        pp = new PreparedPlan();
	        pp.setPlan(plan, copy);
	        pp.setReferences(references);
	        pp.setAnalysisRecord(record);
	        pp.setCommand(newCommand);
	        commandContext.putPlan(query, pp, copy.getDeterminismLevel());
	        copy.setDeterminismLevel(determinismLevel);
		}
		copy.pushVariableContext(new VariableContext());
		PreparedStatementRequest.resolveParameterValues(pp.getReferences(), Arrays.asList(params), copy, metadata);
        return new QueryProcessor(pp.getPlan().clone(), copy, bufferMgr, dataMgr);
	}
}
