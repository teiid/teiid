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

package org.teiid.query.optimizer;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.id.IDGenerator;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.processor.DdlPlan;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.util.CommandContext;

public class DdlPlanner implements CommandPlanner {

    @Override
    public ProcessorPlan optimize(Command command, IDGenerator idGenerator,
            QueryMetadataInterface metadata, CapabilitiesFinder capFinder,
            AnalysisRecord analysisRecord, CommandContext context)
            throws QueryPlannerException, QueryMetadataException,
            TeiidComponentException {
        return new DdlPlan(command);
    }

}
