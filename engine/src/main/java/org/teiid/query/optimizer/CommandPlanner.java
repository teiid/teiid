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
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.util.CommandContext;


/**
 * <p>The common interface of all planners which take a user's command
 * object and produce a
 * {@link org.teiid.query.processor.ProcessorPlan ProcessorPlan}
 * object, which is a plan for executing the query.
 */
public interface CommandPlanner {

    /**
     * Allows the planner a chance to optimize the canonical plan(s) stored in
     * the CommandTreeNode tree.  This method should be called in a bottom-up
     * manner; from leaf nodes up to the root node.
     * @param command TODO
     * @param metadata source of metadata
     * @param capFinder Class usable to find the connector capabilities for a particular model
     * @param context
     * @return ProcessorPlan implementation specific to the CommandPlanner
     * @throws QueryPlannerException indicating a problem in planning
     * @throws QueryMetadataException indicating an exception in accessing the metadata
     * @throws TeiidComponentException indicating an unexpected exception
     */
    ProcessorPlan optimize(Command command, IDGenerator idGenerator, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, AnalysisRecord analysisRecord, CommandContext context)
    throws QueryPlannerException, QueryMetadataException, TeiidComponentException;

}
