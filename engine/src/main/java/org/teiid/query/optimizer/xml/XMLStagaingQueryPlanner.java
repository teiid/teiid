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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.mapping.xml.MappingBaseNode;
import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.MappingNode;
import org.teiid.query.mapping.xml.MappingSourceNode;
import org.teiid.query.mapping.xml.MappingVisitor;
import org.teiid.query.mapping.xml.Navigator;
import org.teiid.query.mapping.xml.ResultSetInfo;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.GroupBy;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Option;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
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
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException, QueryResolverException, QueryParserException {
    
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
        if (rsInfo.hasInputSet() && !rsInfo.isCriteriaRaised()) {
            return false;
        }
        
        // no bindings - no references
        QueryNode queryNode = QueryUtil.getQueryNode(groupName, planEnv.getGlobalMetadata());
        if (queryNode.getBindings() != null && !queryNode.getBindings().isEmpty()) {
            return false;
        }
        
        // get the original transformation of the mapping class before planning. 
        Query stagableQuery = (Query)QueryUtil.getQueryFromQueryNode(groupName, planEnv);
        
        Collection<GroupSymbol> groups = GroupCollectorVisitor.getGroupsIgnoreInlineViews(stagableQuery, false);
        
        //check for already staged queries
        if (groups.size() == 1) {
            GroupSymbol group = groups.iterator().next();
            group = QueryUtil.createResolvedGroup(group.clone(), planEnv.getGlobalMetadata());
            if (planEnv.isStagingTable(group.getMetadataID()) && stagableQuery.getCriteria() == null) {
                return false;
            }
        }
        
        Criteria crit = ((Query)rsInfo.getCommand()).getCriteria();

        GroupSymbol parent = null;
        LinkedHashSet<ElementSymbol> outerReferences = new LinkedHashSet<ElementSymbol>();
        LinkedHashSet<ElementSymbol> fkColumns = new LinkedHashSet<ElementSymbol>();
        //see if we can perform a dependent join
        for (Criteria conjunct : Criteria.separateCriteriaByAnd(crit)) {
        	if (!(conjunct instanceof CompareCriteria)) {
        		continue;
        	}
        	CompareCriteria cc = (CompareCriteria)conjunct;
        	if (cc.getOperator() != CompareCriteria.EQ) {
        		continue;
        	}
        	if (!(cc.getLeftExpression() instanceof ElementSymbol) 
        			|| !(cc.getRightExpression() instanceof ElementSymbol)) {
        		continue;
        	}
    		ElementSymbol les = (ElementSymbol)cc.getLeftExpression();
    		ElementSymbol res = (ElementSymbol)cc.getRightExpression();
    		if (les.getGroupSymbol().getNonCorrelationName().equalsIgnoreCase(groupName)) {
				parent = res.getGroupSymbol();
				outerReferences.add(res.clone());
				fkColumns.add(les.clone());
    		} else if (res.getGroupSymbol().getNonCorrelationName().equalsIgnoreCase(groupName)) {
				parent = les.getGroupSymbol();
				outerReferences.add(les.clone());
				fkColumns.add(res.clone());
    		}
        }
        String stagingGroupName = planEnv.getStagedResultName(groupName);  

        boolean recursive = false;
        MappingSourceNode msn = sourceNode;
        while (!recursive && msn != null) {
        	MappingNode mappingNode = msn.getChildren().get(0);
			if (mappingNode instanceof MappingBaseNode) {
            	recursive = ((MappingBaseNode)mappingNode).isRootRecursiveNode();
        	}
        	msn = msn.getParentSourceNode();
        }
        
        if (parent != null && !recursive) {
            stagableQuery = (Query)stagableQuery.clone();
            String parentName = parent.getNonCorrelationName();
            
            String parentStagingName = planEnv.getStagedResultName(parentName);
            GroupSymbol parentTempTable = new GroupSymbol(XMLQueryPlanner.getTempTableName(parentStagingName));
        	ResultSetInfo parentRsInfo = planEnv.getStagingTableResultsInfo(parentStagingName);
        	String stagingRoot = sourceNode.getParentSourceNode().getSource();
        	
        	boolean parentStaged = parentRsInfo.getPlan() != null;
        	//TODO: check to see if the parent was manually staged
        	
        	if (!parentStaged) {
                //TODO: if not a level 1 child we could use the old auto staging logic instead
        		
        		//switch the parent over to the source
        		parentRsInfo = sourceNode.getParentSourceNode().getResultSetInfo();
        		
        		if (parentRsInfo.getTempTable() == null) {
	        		//create a temp table to represent the resultset
	                List<SingleElementSymbol> projectedSymbols = parentRsInfo.getCommand().getProjectedSymbols();
	                ArrayList<SingleElementSymbol> elements = new ArrayList<SingleElementSymbol>(projectedSymbols.size());
	        		for (SingleElementSymbol singleElementSymbol : projectedSymbols) {
	        			singleElementSymbol = (SingleElementSymbol) singleElementSymbol.clone();
						ResolverVisitor.resolveLanguageObject(singleElementSymbol, planEnv.getGlobalMetadata());
						elements.add(singleElementSymbol);
					}
	                TempMetadataStore store = planEnv.getGlobalMetadata().getMetadataStore();
	                // create a new group name and to the temp store
	                GroupSymbol newGroup = new GroupSymbol(SourceNodePlannerVisitor.getNewName("#" + planEnv.getAliasName(parentName) + "_RS", store)); //$NON-NLS-1$ //$NON-NLS-2$
	                newGroup.setMetadataID(store.addTempGroup(newGroup.getName(), elements, false, true));
	
	                parentStagingName = newGroup.getName();
	                parentTempTable = newGroup;
        		} else {
        			parentStagingName = parentRsInfo.getTempTable();
        			parentTempTable = new GroupSymbol(parentRsInfo.getTempTable());
	                parentStaged = true;
        		}
        	} else {
        		stagingRoot = parentRsInfo.getStagingRoot();
        	}

            Query query = new Query();
            query.setSelect(new Select(Arrays.asList(new ExpressionSymbol("expr", new Constant(1))))); //$NON-NLS-1$

            query.setFrom(new From(Arrays.asList(new UnaryFromClause(parentTempTable))));
            
            Map symbolMap = new HashMap();
            String inlineViewName = planEnv.getAliasName(rsInfo.getResultSetName());
            XMLQueryPlanner.updateSymbolMap(symbolMap, rsInfo.getResultSetName(), inlineViewName, planEnv.getGlobalMetadata());
            XMLQueryPlanner.updateSymbolMap(symbolMap, parentName, parentTempTable.getName(), planEnv.getGlobalMetadata());
            
            crit = (Criteria) crit.clone();
            ExpressionMappingVisitor.mapExpressions(crit, symbolMap);
            
            if (!stagableQuery.getSelect().isDistinct()) {
            	query.setHaving(crit);
                //group by is added so that subquery planning sees that we are distinct
                query.setGroupBy(new GroupBy(new ArrayList<ElementSymbol>(outerReferences)));
                ExpressionMappingVisitor.mapExpressions(query.getGroupBy(), symbolMap);
            } else {
            	query.setCriteria(crit);
            }
            ExistsCriteria ec = new ExistsCriteria();
            ec.setSubqueryHint(new ExistsCriteria.SubqueryHint());
            ec.getSubqueryHint().setDepJoin(true);
            ec.setCommand(query);
            Criteria existing = stagableQuery.getCriteria();
            stagableQuery.setCriteria(Criteria.combineCriteria(existing, ec));
            if (!XMLQueryPlanner.planStagaingQuery(false, groupName, stagingGroupName, stagableQuery, planEnv)) {
            	return false;
            }
            if (!parentStaged) {
            	//need to associate temp load/get/drop with the rsinfo for use by the execsqlinstruction
            	Insert insert = new Insert();
            	insert.setGroup(parentTempTable);
            	int valCount = parentRsInfo.getCommand().getProjectedSymbols().size();
            	ArrayList<Reference> vals = new ArrayList<Reference>(valCount);
            	for (int i = 0; i < valCount; i++) {
            		vals.add(new Reference(i+1));
            	}
            	insert.setValues(vals);
	        	QueryResolver.resolveCommand(insert, planEnv.getGlobalMetadata());
	        	
	        	Command tempCommand = QueryParser.getQueryParser().parseCommand("select * from " + parentStagingName); //$NON-NLS-1$
	        	QueryResolver.resolveCommand(tempCommand, planEnv.getGlobalMetadata());
	        	
            	Command dropCommand = QueryParser.getQueryParser().parseCommand("drop table " + parentStagingName); //$NON-NLS-1$
	        	QueryResolver.resolveCommand(dropCommand, planEnv.getGlobalMetadata());

	            parentRsInfo.setTempTable(parentStagingName);
	            parentRsInfo.setTempSelect(tempCommand);
	            parentRsInfo.setTempInsert(insert);
	            parentRsInfo.setTempDrop(dropCommand);
            }
            LogManager.logDetail(LogConstants.CTX_XML_PLANNER, "Using a dependent join to load the mapping class", groupName); //$NON-NLS-1$
            // add to the document that a staging table has been added
            sourceNode.addStagingTable(stagingGroupName);
            GroupSymbol tempGroup = new GroupSymbol(XMLQueryPlanner.getTempTableName(stagingGroupName));
        	ResolverUtil.resolveGroup(tempGroup, planEnv.getGlobalMetadata());
        	Collection<GroupSymbol> temp = Arrays.asList(tempGroup);
        	List<ElementSymbol> fk = new ArrayList<ElementSymbol>(fkColumns.size());
        	for (ElementSymbol elementSymbol : fkColumns) {
        		ElementSymbol es = new ElementSymbol(elementSymbol.getShortName());
        		ResolverVisitor.resolveLanguageObject(es, temp, planEnv.getGlobalMetadata());
        		fk.add(es);
			}
            ResultSetInfo stagedInfo = planEnv.getStagingTableResultsInfo(stagingGroupName);
        	stagedInfo.setStagingRoot(stagingRoot);
        	stagedInfo.setAutoStaged(true);
        	stagedInfo.setFkColumns(fk);
        	stagedInfo.setTempTable(tempGroup.getName());
        	
        	rsInfo.setAutoStaged(true);
            return true;
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

        stagableQuery = (Query)stagableQuery.clone();

        // stage the transformation query and it is successful
        if (!XMLQueryPlanner.planStagaingQuery(true, groupName, stagingGroupName, stagableQuery, planEnv)) {
            return false;
        }
        
        // add to the document that a staging table has been added
        sourceNode.addStagingTable(stagingGroupName);
        ResultSetInfo stagedInfo = planEnv.getStagingTableResultsInfo(stagingGroupName);
    	stagedInfo.setAutoStaged(true);
    	stagedInfo.setTempTable(XMLQueryPlanner.getTempTableName(stagingGroupName));
    	
    	rsInfo.setAutoStaged(true);
        return true;
    }

}
