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

package org.teiid.query.optimizer.xml;

import java.util.HashMap;
import java.util.Map;

import org.teiid.core.id.IDGenerator;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.ResultSetInfo;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.processor.xml.Program;
import org.teiid.query.processor.xml.XMLProcessorEnvironment;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.util.CommandContext;


/**
 * This handy little class simply holds data that is sent recursively throughout
 * the XMLPlanner.  It is useful to define it all here, where more data can be
 * added as needed, rather than change parameters to all the method calls
 * later on.  A single instance will be used in XMLPlanner.
 */
public final class XMLPlannerEnvironment{

    /**
     * Property key to indicate what form the XML result documents should be in, 
     * either String (default) or JDOM document.  The value of the property
     * should be one of 
     * {@link ProcessorEnvironment#STRING_RESULT} or
     * {@link ProcessorEnvironment#JDOM_DOCUMENT_RESULT} or
     * This is to be used in conjunction with 
     * property methods.
     */
    public static final Integer XML_FORM_RESULTS_PROPERTY = new Integer(0); 
            

    // ################## Resources ################## 

    // Helper to find capabilities for data sources
    CapabilitiesFinder capFinder;
    
    // The metadata
    private QueryMetadataInterface metadata;
    
    // An IDGenerator for plan nodes
    IDGenerator idGenerator;
    
    // ################## Initialization state ################## 
    
    // The group symbol representing the document itself
    GroupSymbol documentGroup; 
    
    MappingDocument mappingDoc;
    
    // The original XML query command
    Query xmlCommand;
    
    // Record of planning decisions
    AnalysisRecord analysisRecord;
    
    // Context this command is running in
    CommandContext context;
    
    // ################## Planning state ################## 
    private HashMap stagingResultsInfo = new HashMap();
    
    /**
     * Global temp metadata - dynamically generated mapping classes and staging tables should 
     * be defined here so that they are known to all for resolution.  This map should be 
     * used with a TempMetadataStore and a TempMetadataAdapter.  
     */
    private Map globalTempMetadata = new HashMap();
    
    private Map stagingTableMap = new HashMap();
    
    private Map queryNodeMap = new HashMap();
    
    // ################## Output for processing -> XMLPlan ##################
    
    public XMLPlannerEnvironment(QueryMetadataInterface qmi) {
        this.metadata = qmi;
    }

    XMLProcessorEnvironment createProcessorEnvironment(Program mainProgram) {
        XMLProcessorEnvironment processorEnv = new XMLProcessorEnvironment(mainProgram);
        
        processorEnv.setDocumentGroup(documentGroup);
        return processorEnv;
    }
      
        
    TempMetadataAdapter getGlobalMetadata() {
        return new TempMetadataAdapter(metadata, new TempMetadataStore(this.globalTempMetadata), this.stagingTableMap, this.queryNodeMap);
    }

    public ResultSetInfo getStagingTableResultsInfo(String groupName) {
        ResultSetInfo info = (ResultSetInfo)this.stagingResultsInfo.get(groupName.toUpperCase());
        if (info == null) {
            info = new ResultSetInfo(groupName);            
            this.stagingResultsInfo.put(info.getResultSetName().toUpperCase(), info);
        }
        return info;
    }
    
    /**
     * Dynamically setting up the staging tables as meterialized views.  
     * @param groupSymbol
     * @param intoGroupSymbol
     */
    public void addStagingTable(Object groupId, Object intoGroupId) {
        this.stagingTableMap.put(groupId, intoGroupId);
    }
    
    public boolean isStagingTable(Object groupId) {
        return this.stagingTableMap.containsKey(groupId);
    }

    public void addToGlobalMetadata(Map data) {
        this.globalTempMetadata.putAll(data);
    }
    
    public void addQueryNodeToMetadata(Object metadataId, QueryNode node) {
        this.queryNodeMap.put(metadataId, node);
    }    
    
    public String unLoadResultName(String loadName) {
        return "unload_"+loadName; //$NON-NLS-1$
    }
    
    public String getStagedResultName(String rsName) {
        return "autostaged_"+rsName.replace('.', '_'); //$NON-NLS-1$
    }    
    
    public String getAliasName(final String rsName) {
        String inlineViewName = rsName.replace(ElementSymbol.SEPARATOR.charAt(0), '_');
        return inlineViewName;
    }    
}