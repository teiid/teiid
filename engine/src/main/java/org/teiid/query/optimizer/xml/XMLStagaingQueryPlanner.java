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

import java.util.Collection;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.MappingSourceNode;
import org.teiid.query.mapping.xml.MappingVisitor;
import org.teiid.query.mapping.xml.Navigator;
import org.teiid.query.mapping.xml.ResultSetInfo;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.sql.lang.Option;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;


/** 
 * This class has code to with planning the automatic XML staging queries.
 */
public class XMLStagaingQueryPlanner {
    
    
    static void stageQueries(MappingDocument doc, final XMLPlannerEnvironment planEnv) 
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        MappingVisitor queryPlanVisitor = new MappingVisitor() {
            public void visit(MappingSourceNode sourceNode) {
                try {
                    stagePlannedQuery(sourceNode, planEnv);
                } catch (Exception e) {
                    throw new TeiidRuntimeException(e);
                }
            }
        };
        planWalk(doc, queryPlanVisitor);
    }
    
    private static void planWalk(MappingDocument doc, MappingVisitor visitor) 
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {
    
        try {
            Navigator walker = new Navigator(true, visitor);
            doc.acceptVisitor(walker);
        } catch (TeiidRuntimeException e) {
            if (e.getCause() instanceof QueryPlannerException) {
                throw (QueryPlannerException)e.getCause();
            }           
            else if (e.getCause() instanceof QueryMetadataException) {
                throw (QueryMetadataException)e.getCause();
            }
            else if (e.getCause() instanceof TeiidComponentException) {
                throw (TeiidComponentException)e.getCause();
            }
            else {
                throw e;
            }
        }
    }    
    
    static boolean stagePlannedQuery(MappingSourceNode sourceNode, XMLPlannerEnvironment planEnv) 
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException, QueryResolverException {
    
        Option option = planEnv.xmlCommand.getOption();
        
        // make sure we do not plan staging table for root mapping class.
        if (sourceNode.isRootSourceNode()) {
            return false;
        }
        
        //TODO: if this source is source for a recursive node, we do not want to stage
        if (sourceNode.getAliasResultName() != null) {
            return false;
        }
        
        String groupName = sourceNode.getActualResultSetName();
        ResultSetInfo rsInfo = sourceNode.getResultSetInfo();

        // If this node has been eligible for raising, it will be eligible for staging.
        if (!rsInfo.isCriteriaRaised()) {
            return false;
        }
        
        // no bindings - no references
        QueryNode queryNode = QueryUtil.getQueryNode(groupName, planEnv.getGlobalMetadata());
        if (queryNode.getBindings() != null && !queryNode.getBindings().isEmpty()) {
            return false;
        }
        
        //id as mapping class
        Object metadataID = planEnv.getGlobalMetadata().getGroupID(sourceNode.getResultName());
        if (RelationalPlanner.isNoCacheGroup(planEnv.getGlobalMetadata(), metadataID, option)) {
            return false;
        }

        //id as generated mapping class name
        metadataID = planEnv.getGlobalMetadata().getGroupID(sourceNode.getActualResultSetName());
        if (RelationalPlanner.isNoCacheGroup(planEnv.getGlobalMetadata(), metadataID, option)) {
            return false;
        }
        
        // get the original transformation of the mapping class before planning. 
        Query stagableQuery = (Query)QueryUtil.getQueryFromQueryNode(groupName, planEnv);
        
        Collection<GroupSymbol> groups = GroupCollectorVisitor.getGroupsIgnoreInlineViews(stagableQuery, false);
        
        //check for already staged queries
        if (groups.size() == 1) {
            GroupSymbol group = groups.iterator().next();
            group = QueryUtil.createResolvedGroup((GroupSymbol)group.clone(), planEnv.getGlobalMetadata());
            if (planEnv.isStagingTable(group.getMetadataID()) && stagableQuery.getCriteria() == null) {
                return false;
            }
        }
        
        stagableQuery = (Query)stagableQuery.clone();
                
        // stage the transformation query and it is successful
        String stagingGroupName = planEnv.getStagedResultName(groupName);         
        if (!XMLQueryPlanner.planStagaingQuery(true, groupName, stagingGroupName, stagableQuery, planEnv)) {
            return false;
        }
        
        // add to the document that a staging table has been added
        sourceNode.addStagingTable(stagingGroupName);
        
        return true;
    }

}
