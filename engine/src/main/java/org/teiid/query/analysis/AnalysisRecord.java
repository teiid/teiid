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

package org.teiid.query.analysis;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

import org.teiid.client.plan.Annotation;
import org.teiid.client.plan.PlanNode;
import org.teiid.core.types.DataTypeManager;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;


/**
 * <p>The AnalysisRecord holds all debug/analysis information for 
 * a particular query as it is executed.  This includes:</p>
 * <UL>
 * <LI>Flags indicating what should be recorded</LI>
 * <LI>Query plan, if requested</LI>
 * <LI>Annotations indicating important decisions, if requested</li>
 * <li>Debug trace information, if requested</LI>
 * </ul>
 */
public class AnalysisRecord {
	
    // Common 
    public static final String PROP_OUTPUT_COLS = "Output Columns"; //$NON-NLS-1$
    
    // Relational
    public static final String PROP_CRITERIA = "Criteria"; //$NON-NLS-1$
    public static final String PROP_SELECT_COLS = "Select Columns"; //$NON-NLS-1$
    public static final String PROP_GROUP_COLS = "Grouping Columns"; //$NON-NLS-1$
    public static final String PROP_SQL = "Query"; //$NON-NLS-1$
    public static final String PROP_MODEL_NAME = "Model Name"; //$NON-NLS-1$
    public static final String PROP_DEPENDENT = "Dependent Join"; //$NON-NLS-1$
    public static final String PROP_JOIN_STRATEGY = "Join Strategy"; //$NON-NLS-1$
    public static final String PROP_JOIN_TYPE = "Join Type"; //$NON-NLS-1$
    public static final String PROP_JOIN_CRITERIA = "Join Criteria"; //$NON-NLS-1$
    public static final String PROP_EXECUTION_PLAN = "Execution Plan"; //$NON-NLS-1$
    public static final String PROP_INTO_GROUP = "Into Target"; //$NON-NLS-1$
    public static final String PROP_SORT_COLS = "Sort Columns"; //$NON-NLS-1$
    public static final String PROP_SORT_MODE = "Sort Mode"; //$NON-NLS-1$
    public static final String PROP_NODE_STATS_LIST = "Statistics"; //$NON-NLS-1$
    public static final String PROP_NODE_COST_ESTIMATES = "Cost Estimates";  //$NON-NLS-1$
    public static final String PROP_ROW_OFFSET = "Row Offset";  //$NON-NLS-1$
    public static final String PROP_ROW_LIMIT = "Row Limit";  //$NON-NLS-1$
    public static final String PROP_WITH = "With"; //$NON-NLS-1$
    
    // XML
    public static final String PROP_MESSAGE = "Message"; //$NON-NLS-1$
    public static final String PROP_TAG = "Tag"; //$NON-NLS-1$
    public static final String PROP_NAMESPACE = "Namespace"; //$NON-NLS-1$
    public static final String PROP_DATA_COL = "Data Column"; //$NON-NLS-1$
    public static final String PROP_NAMESPACE_DECL = "Namespace Declarations"; //$NON-NLS-1$
    public static final String PROP_OPTIONAL = "Optional Flag"; //$NON-NLS-1$
    public static final String PROP_DEFAULT = "Default Value"; //$NON-NLS-1$
    public static final String PROP_RECURSE_DIR = "Recursion Direction";  //$NON-NLS-1$
    public static final String PROP_BINDINGS = "Bindings"; //$NON-NLS-1$
    public static final String PROP_IS_STAGING = "Is Staging Flag"; //$NON-NLS-1$
    public static final String PROP_IN_MEMORY = "Source In Memory Flag"; //$NON-NLS-1$
    public static final String PROP_CONDITION = "Condition"; //$NON-NLS-1$
    public static final String PROP_DEFAULT_PROGRAM = "Default Program"; //$NON-NLS-1$
    public static final String PROP_ENCODING = "Encoding"; //$NON-NLS-1$
    public static final String PROP_FORMATTED = "Formatted Flag"; //$NON-NLS-1$
    
    // Procedure
    public static final String PROP_EXPRESSION = "Expression"; //$NON-NLS-1$
    public static final String PROP_RESULT_SET = "Result Set"; //$NON-NLS-1$
    public static final String PROP_PROGRAM = "Program";  //$NON-NLS-1$
    public static final String PROP_VARIABLE = "Variable"; //$NON-NLS-1$
    public static final String PROP_THEN = "Then"; //$NON-NLS-1$
    public static final String PROP_ELSE = "Else"; //$NON-NLS-1$

    // Flags regarding what should be recorded
    private boolean recordQueryPlan;
    private boolean recordDebug;
    
    // Annotations
    private Collection<Annotation> annotations;
    
    // Debug trace log
    private StringWriter stringWriter;  // inner
    private PrintWriter debugWriter;    // public
    
    public AnalysisRecord(boolean recordQueryPlan, boolean recordDebug) {
    	this.recordQueryPlan = recordQueryPlan | LogManager.isMessageToBeRecorded(LogConstants.CTX_QUERY_PLANNER, MessageLevel.DETAIL);
        this.recordDebug = recordDebug | LogManager.isMessageToBeRecorded(LogConstants.CTX_QUERY_PLANNER, MessageLevel.TRACE);
        
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
    
    /**
     * Add an annotation.  This can only be used if {@link #recordAnnotations}
     * returns true.
     * @param annotation Annotation to add
     */
    public void addAnnotation(Annotation annotation) {
        this.annotations.add(annotation);
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
	public static List<String> getOutputColumnProperties(List<? extends SingleElementSymbol> projectedSymbols) {
	    if(projectedSymbols != null) {
	        List<String> outputCols = new ArrayList<String>(projectedSymbols.size());
	        for(int i=0; i<projectedSymbols.size() ; i++) {
	            SingleElementSymbol symbol = projectedSymbols.get(i);
	            outputCols.add(symbol.getShortName() + " (" + DataTypeManager.getDataTypeName(symbol.getType()) + ")"); //$NON-NLS-1$ //$NON-NLS-2$
	        }
	        return outputCols;
	    }
	    return Collections.emptyList();
	}
	
	public static void addLanaguageObjects(PlanNode node, String key, List<? extends LanguageObject> objects) {
		List<String> values = new ArrayList<String>();
		int index = 0;
		for (LanguageObject languageObject : objects) {
			values.add(languageObject.toString());
			List<SubqueryContainer> subqueries = ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(objects);
			for (ListIterator<SubqueryContainer> iterator = subqueries.listIterator(); iterator.hasNext();) {
				SubqueryContainer subqueryContainer = iterator.next();
				node.addProperty(key + " Subplan " + index++, subqueryContainer.getCommand().getProcessorPlan().getDescriptionProperties()); //$NON-NLS-1$
			}
		}
		node.addProperty(key, values);
	}
}
