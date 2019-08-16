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

package org.teiid.query.analysis;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

import org.teiid.client.plan.Annotation;
import org.teiid.client.plan.Annotation.Priority;
import org.teiid.client.plan.PlanNode;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;


/**
 * <p>The AnalysisRecord holds all debug/analysis information for
 * a particular query as it is executed.  This includes:
 * <UL>
 * <LI>Flags indicating what should be recorded</LI>
 * <LI>Query plan, if requested</LI>
 * <LI>Annotations indicating important decisions, if requested</li>
 * <li>Debug trace information, if requested</LI>
 * </ul>
 */
public class AnalysisRecord {

    private static final int MAX_PLAN_LENGTH = PropertiesUtils.getHierarchicalProperty("org.teiid.maxPlanLength", 1<<25, Integer.class); //$NON-NLS-1$

    // Common
    public static final String PROP_OUTPUT_COLS = "Output Columns"; //$NON-NLS-1$
    public static final String PROP_ID = "Relational Node ID"; //$NON-NLS-1$
    public static final String PROP_DATA_BYTES_SENT = "Data Bytes Sent"; //$NON-NLS-1$

    // Relational
    public static final String PROP_CRITERIA = "Criteria"; //$NON-NLS-1$
    public static final String PROP_SELECT_COLS = "Select Columns"; //$NON-NLS-1$
    public static final String PROP_GROUP_COLS = "Grouping Columns"; //$NON-NLS-1$
    public static final String PROP_GROUP_MAPPING = "Grouping Mapping"; //$NON-NLS-1$
    public static final String PROP_SQL = "Query"; //$NON-NLS-1$
    public static final String PROP_MODEL_NAME = "Model Name"; //$NON-NLS-1$
    public static final String PROP_SHARING_ID = "Sharing ID"; //$NON-NLS-1$
    public static final String PROP_DEPENDENT = "Dependent Join"; //$NON-NLS-1$
    public static final String PROP_JOIN_STRATEGY = "Join Strategy"; //$NON-NLS-1$
    public static final String PROP_JOIN_TYPE = "Join Type"; //$NON-NLS-1$
    public static final String PROP_JOIN_CRITERIA = "Join Criteria"; //$NON-NLS-1$
    public static final String PROP_EXECUTION_PLAN = "Execution Plan"; //$NON-NLS-1$
    public static final String PROP_INTO_GROUP = "Into Target"; //$NON-NLS-1$
    public static final String PROP_UPSERT = "Upsert"; //$NON-NLS-1$
    public static final String PROP_SORT_COLS = "Sort Columns"; //$NON-NLS-1$
    public static final String PROP_SORT_MODE = "Sort FrameMode"; //$NON-NLS-1$
    public static final String PROP_ROLLUP = "Rollup"; //$NON-NLS-1$
    public static final String PROP_NODE_STATS_LIST = "Statistics"; //$NON-NLS-1$
    public static final String PROP_NODE_COST_ESTIMATES = "Cost Estimates";  //$NON-NLS-1$
    public static final String PROP_ROW_OFFSET = "Row Offset";  //$NON-NLS-1$
    public static final String PROP_ROW_LIMIT = "Row Limit";  //$NON-NLS-1$
    public static final String PROP_WITH = "With"; //$NON-NLS-1$
    public static final String PROP_WINDOW_FUNCTIONS = "Window Functions"; //$NON-NLS-1$
    //Table functions
    public static final String PROP_TABLE_FUNCTION = "Table Function"; //$NON-NLS-1$

    public static final String PROP_STREAMING = "Streaming"; //$NON-NLS-1$

    // Procedure
    public static final String PROP_EXPRESSION = "Expression"; //$NON-NLS-1$
    public static final String PROP_RESULT_SET = "Result Set"; //$NON-NLS-1$
    public static final String PROP_PROGRAM = "Program";  //$NON-NLS-1$
    public static final String PROP_VARIABLE = "Variable"; //$NON-NLS-1$
    public static final String PROP_THEN = "Then"; //$NON-NLS-1$
    public static final String PROP_ELSE = "Else"; //$NON-NLS-1$

    public static final String PROP_PLANNING_TIME = "Planning Time"; //$NON-NLS-1$

    // Flags regarding what should be recorded
    private boolean recordQueryPlan;
    private boolean recordDebug;

    // Annotations
    private Collection<Annotation> annotations;

    // Debug trace log
    private StringWriter stringWriter;  // inner
    private PrintWriter debugWriter;    // public

    public AnalysisRecord(boolean recordQueryPlan, boolean recordDebug) {
        this.recordQueryPlan = recordQueryPlan || LogManager.isMessageToBeRecorded(LogConstants.CTX_QUERY_PLANNER, MessageLevel.DETAIL);
        this.recordDebug = recordDebug || LogManager.isMessageToBeRecorded(LogConstants.CTX_QUERY_PLANNER, MessageLevel.TRACE);

        if(this.recordQueryPlan) {
            this.annotations = new ArrayList<Annotation>();
        }

        if(this.recordDebug) {
            this.stringWriter = new StringWriter();
            this.debugWriter = new PrintWriter(this.stringWriter);
        }
    }

    public static AnalysisRecord createNonRecordingRecord() {
        return new AnalysisRecord(false, false);
    }

    /**
     * Determine whether query plan should be recorded
     * @return True to record
     */
    public boolean recordQueryPlan() {
        return this.recordQueryPlan;
    }

    /**
     * Determine whether annotations should be recorded
     * @return True to record
     */
    public boolean recordAnnotations() {
        return this.recordQueryPlan;
    }

    /**
     * Determine whether debug trace log should be recorded
     * @return True to record
     */
    public boolean recordDebug() {
        return this.recordDebug;
    }

    public void addAnnotation(String category, String annotation, String resolution, Priority priority) {
        addAnnotation(new Annotation(category, annotation, resolution, priority));
    }

    /**
     * Add an annotation.  This can only be used if {@link #recordAnnotations}
     * returns true.
     * @param annotation Annotation to add
     */
    public void addAnnotation(Annotation annotation) {
        this.annotations.add(annotation);
        if (this.recordDebug()) {
            this.println(annotation.toString());
        }
    }

    /**
     * Get annotations.
     * @return
     */
    public Collection<Annotation> getAnnotations() {
        return this.annotations;
    }

    /**
     * Add line to debug log  This can only be
     * used if {@link #recordDebug} returns true.
     * @param debugLine Text to add to debug writer
     */
    public void println(String debugLine) {
        if (this.stringWriter.getBuffer().length() > MAX_PLAN_LENGTH) {
            this.stringWriter.getBuffer().delete(0, this.stringWriter.getBuffer().length() - (MAX_PLAN_LENGTH*3/4));
        }
        this.debugWriter.println(debugLine);
    }

    /**
     * Get debug trace log recorded to writer.  Typically this is used
     * once at the end of query execution.
     * @return
     */
    public String getDebugLog() {
        if(recordDebug) {
            return this.stringWriter.getBuffer().toString();
        }
        return null;
    }

    public void stopDebugLog() {
        this.stringWriter = null;
        this.recordDebug = false;
    }

    /**
     * Helper method to turn a list of projected symbols into a suitable list of
     * output column strings with name and type.
     * @param projectedSymbols The list of SingleElementSymbol projected from a plan or node
     * @return List of output columns for sending to the client as part of the plan
     */
    public static List<String> getOutputColumnProperties(List<? extends Expression> projectedSymbols) {
        if(projectedSymbols != null) {
            List<String> outputCols = new ArrayList<String>(projectedSymbols.size());
            for(int i=0; i<projectedSymbols.size() ; i++) {
                Expression symbol = projectedSymbols.get(i);
                outputCols.add(Symbol.getShortName(symbol) + " (" + DataTypeManager.getDataTypeName(symbol.getType()) + ")"); //$NON-NLS-1$ //$NON-NLS-2$
            }
            return outputCols;
        }
        return Collections.emptyList();
    }

    public static void addLanaguageObjects(PlanNode node, String key, Collection<? extends LanguageObject> objects) {
        List<String> values = new ArrayList<String>();
        int index = 0;
        for (LanguageObject languageObject : objects) {
            values.add(languageObject.toString());
            List<SubqueryContainer<?>> subqueries = ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(languageObject);
            for (ListIterator<SubqueryContainer<?>> iterator = subqueries.listIterator(); iterator.hasNext();) {
                SubqueryContainer<?> subqueryContainer = iterator.next();
                node.addProperty(key + " Subplan " + index++, subqueryContainer.getCommand().getProcessorPlan().getDescriptionProperties()); //$NON-NLS-1$
            }
        }
        node.addProperty(key, values);
    }
}
