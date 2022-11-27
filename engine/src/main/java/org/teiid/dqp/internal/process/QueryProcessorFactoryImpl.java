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

package org.teiid.dqp.internal.process;

import java.util.Arrays;
import java.util.List;

import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.id.IDGenerator;
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

    private QueryMetadataInterface defaultMetadata;
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
        this.defaultMetadata = metadata;
    }

    @Override
    public QueryProcessor createQueryProcessor(String query, String recursionGroup, CommandContext commandContext, Object... params) throws TeiidProcessingException, TeiidComponentException {
        CommandContext copy = commandContext.clone();
        copy.resetDeterminismLevel(true);
        copy.setDataObjects(null);
        QueryMetadataInterface metadata = commandContext.getMetadata();
        if (metadata == null) {
            metadata = defaultMetadata;
        }
        PreparedPlan pp = getPreparedPlan(query, recursionGroup, copy, metadata);
        copy.pushVariableContext(new VariableContext());
        PreparedStatementRequest.resolveParameterValues(pp.getReferences(), Arrays.asList(params), copy, metadata);
        return new QueryProcessor(pp.getPlan().clone(), copy, bufferMgr, dataMgr);
    }

    @Override
    public PreparedPlan getPreparedPlan(String query, String recursionGroup,
            CommandContext commandContext, QueryMetadataInterface metadata) throws
            TeiidComponentException, TeiidProcessingException {
        if (recursionGroup != null) {
            commandContext.pushCall(recursionGroup);
        }
        PreparedPlan pp = commandContext.getPlan(query);
        if (pp == null) {
            ParseInfo parseInfo = new ParseInfo();
            Command newCommand = QueryParser.getQueryParser().parseCommand(query, parseInfo);
            QueryResolver.resolveCommand(newCommand, metadata);

            List<Reference> references = ReferenceCollectorVisitor.getReferences(newCommand);

            AbstractValidationVisitor visitor = new ValidationVisitor();
            Request.validateWithVisitor(visitor, metadata, newCommand);
            newCommand = QueryRewriter.rewrite(newCommand, metadata, commandContext);
            AnalysisRecord record = new AnalysisRecord(false, false);
            ProcessorPlan plan = QueryOptimizer.optimizePlan(newCommand, metadata, idGenerator, finder, record, commandContext);
            pp = new PreparedPlan();
            pp.setPlan(plan, commandContext);
            pp.setReferences(references);
            pp.setAnalysisRecord(record);
            pp.setCommand(newCommand);
            commandContext.putPlan(query, pp, commandContext.getDeterminismLevel());
        }
        return pp;
    }

    @Override
    public CapabilitiesFinder getCapabiltiesFinder() {
        return finder;
    }

}
