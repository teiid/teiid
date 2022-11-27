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

import java.util.List;
import java.util.Properties;

import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.deployers.TranslatorUtil;
import org.teiid.dqp.internal.datamgr.CapabilitiesConverter;
import org.teiid.dqp.message.RequestID;
import org.teiid.metadata.index.VDBMetadataFactory;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.QueryOptimizer;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.ExecutionFactory;

import junit.framework.TestCase;



/**
 * @since 4.2
 */
public abstract class BaseQueryTest extends TestCase {

    public BaseQueryTest(String name) {
        super(name);
    }

    public static SourceCapabilities getCapabilities(Class<? extends ExecutionFactory> clazz, String... properties) throws Exception {
        VDBTranslatorMetaData tm = new VDBTranslatorMetaData();

        tm.setExecutionFactoryClass(clazz);

        if (properties != null) {
            Properties p = new Properties();
            for (int i = 0; i < properties.length / 2; i++) {
                tm.addProperty(properties[2*i], properties[2*i+1]);
            }
        }
        ExecutionFactory ef = TranslatorUtil.buildExecutionFactory(tm);
        return CapabilitiesConverter.convertCapabilities(ef);
    }

    public static TransformationMetadata createMetadata(String vdbFile) {
        return VDBMetadataFactory.getVDBMetadata(vdbFile);
    }

    protected void doProcess(QueryMetadataInterface metadata, String sql, CapabilitiesFinder capFinder, ProcessorDataManager dataManager, List[] expectedResults, boolean debug) throws Exception {
        CommandContext context = createCommandContext();
        BufferManagerImpl bm = BufferManagerFactory.createBufferManager();
        bm.setProcessorBatchSize(context.getProcessorBatchSize());
        context.setBufferManager(bm);
        doProcess(metadata, sql, capFinder, dataManager, expectedResults,
                debug, context);
    }

    protected void doProcess(QueryMetadataInterface metadata, String sql,
            CapabilitiesFinder capFinder, ProcessorDataManager dataManager,
            List[] expectedResults, boolean debug, CommandContext context)
            throws TeiidComponentException, TeiidProcessingException,
            QueryMetadataException, QueryPlannerException, Exception {
        Command command = TestOptimizer.helpGetCommand(sql, metadata);

        // plan
        AnalysisRecord analysisRecord = new AnalysisRecord(false, debug);
        ProcessorPlan plan = null;
        try {
            plan = QueryOptimizer.optimizePlan(command, metadata, null, capFinder, analysisRecord, context);
        } finally {
            if(debug) {
                System.out.println(analysisRecord.getDebugLog());
            }
        }

        TestProcessor.doProcess(plan, dataManager, expectedResults, context);
    }

    protected CommandContext createCommandContext() {
        CommandContext context = new CommandContext(new RequestID(), "test", "user", "myvdb", 1); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        return context;
    }

}
