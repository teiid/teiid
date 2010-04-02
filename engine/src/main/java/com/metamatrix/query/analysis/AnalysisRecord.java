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

package com.metamatrix.query.analysis;

import java.io.*;
import java.util.*;

import org.teiid.client.plan.Annotation;

import com.metamatrix.common.log.LogConstants;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.log.MessageLevel;

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
public class AnalysisRecord implements Serializable {

    // Flags regarding what should be recorded
    private boolean recordQueryPlan;
    private boolean recordDebug;
    private boolean logDebug;
    
    // Query plan
    private Map queryPlan;
    
    // Annotations
    private Collection<Annotation> annotations;
    
    // Debug trace log
    private StringWriter stringWriter;  // inner
    private PrintWriter debugWriter;    // public
    
    public AnalysisRecord(boolean recordQueryPlan, boolean recordDebug) {
        this.recordQueryPlan = recordQueryPlan | LogManager.isMessageToBeRecorded(LogConstants.CTX_QUERY_PLANNER, MessageLevel.DETAIL);
        this.logDebug = recordDebug;
        this.recordDebug = recordDebug | LogManager.isMessageToBeRecorded(LogConstants.CTX_QUERY_PLANNER, MessageLevel.TRACE);
        
        if(this.recordQueryPlan) {
            this.annotations = new ArrayList<Annotation>();
        }
        
        if(this.recordDebug) {
            this.stringWriter = new StringWriter();
            this.debugWriter = new PrintWriter(this.stringWriter); 
        }
    }
    
    public boolean logDebug() {
		return logDebug;
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
     * Set the query plan that was created
     * @param queryPlan The plan
     */
    public void setQueryPlan(Map queryPlan) {
        this.queryPlan = queryPlan;
    }
    
    /**
     * Get the query plan that was created
     * @return The plan
     */
    public Map getQueryPlan() {
        return this.queryPlan;
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
}
