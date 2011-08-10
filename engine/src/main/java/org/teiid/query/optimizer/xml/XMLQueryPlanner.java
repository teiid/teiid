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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.query.QueryPlugin;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.mapping.xml.MappingBaseNode;
import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.MappingSourceNode;
import org.teiid.query.mapping.xml.MappingVisitor;
import org.teiid.query.mapping.xml.Navigator;
import org.teiid.query.mapping.xml.ResultSetInfo;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.optimizer.QueryOptimizer;
import org.teiid.query.optimizer.relational.rules.NewCalculateCostUtil;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Drop;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.GroupContext;
import org.teiid.query.sql.lang.Into;
import org.teiid.query.sql.lang.JoinPredicate;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.lang.SubqueryFromClause;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.sql.visitor.PredicateCollectorVisitor;
import org.teiid.query.sql.visitor.StaticSymbolMappingVisitor;


public class XMLQueryPlanner {

    private static final class MappingSourceNodeFinder extends MappingVisitor {
		private final GroupSymbol gs;
		MappingSourceNode msn;

		private MappingSourceNodeFinder(GroupSymbol gs) {
			this.gs = gs;
		}

		@Override
		public void visit(MappingSourceNode element) {
			if (element.getAliasResultName() == null && element.getResultSetInfo().getResultSetName().equalsIgnoreCase(gs.getNonCorrelationName())) {
				msn = element;
				setAbort(true);
			}
		}
	}

	static void prePlanQueries(MappingDocument doc, final XMLPlannerEnvironment planEnv) 
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {
        
        MappingVisitor queryPlanVisitor = new MappingVisitor() {
            
            public void visit(MappingBaseNode baseNode) {
                try {
                    // first if there are any explicit staging tables plan them first 
                    List<String> stagingTables = baseNode.getStagingTables();
                    for (String tableName : stagingTables) {
                        planStagingTable(tableName, planEnv);    
                    }
                    
                    // now if this is of they source node plan; all other nodes
                    // do not need query planning.
                    if (baseNode instanceof MappingSourceNode) {
                        planQueries((MappingSourceNode)baseNode, planEnv);
                    }
                    
                } catch (Exception e) {
                    throw new TeiidRuntimeException(e);
                } 
            }     
            
        };        
        planWalk(doc, queryPlanVisitor);
    }
    
    static void optimizeQueries(MappingDocument doc, final XMLPlannerEnvironment planEnv) 
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        MappingVisitor queryPlanVisitor = new MappingVisitor() {
            public void visit(MappingSourceNode sourceNode) {
                try {
                    ResultSetInfo rsInfo = sourceNode.getResultSetInfo();
                
                    Query command = (Query)rsInfo.getCommand();
                    
                    prepareQuery(sourceNode, planEnv, command);
                    
                    Command cmd = QueryUtil.rewriteQuery(command, planEnv.getGlobalMetadata(), planEnv.context);
                    
                    // Plan the result set.
                    ProcessorPlan queryPlan = optimizePlan(cmd, planEnv);
                    rsInfo.setPlan(queryPlan);                    
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
    
    static void planQueries(final MappingSourceNode sourceNode, XMLPlannerEnvironment planEnv) 
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        ResultSetInfo rsInfo = sourceNode.getResultSetInfo();
        
        // Create sql:  SELECT * FROM rsName            
        Query rsQuery = (Query)rsInfo.getCommand();
        
        // add user order by to base query
        rsQuery.setOrderBy(rsInfo.getOrderBy());

        // add user criteria to base query from model
        Criteria crit = rsInfo.getCriteria();
        try {
            if(crit != null) {
                planQueryWithCriteria(sourceNode, planEnv);
            }
        } catch (QueryResolverException e) {
            throw new TeiidComponentException(e);
        }

        if (rsInfo.getUserRowLimit() != -1) {
            int limit = rsInfo.getUserRowLimit();
            if (rsInfo.exceptionOnRowlimit()) {
                limit++;
            }
            rsQuery.setLimit(new Limit(null, new Constant(new Integer(limit))));
        }
        
        // this query is not eligible for staging; proceed normally.
        rsInfo.setCommand(rsQuery);            
    }
      
    static ProcessorPlan optimizePlan(Command query, XMLPlannerEnvironment planEnv)
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        TempMetadataAdapter metadata = planEnv.getGlobalMetadata();
        ProcessorPlan plan = QueryOptimizer.optimizePlan(query, metadata, planEnv.idGenerator, planEnv.capFinder, planEnv.analysisRecord, planEnv.context);
    
        return plan;
    }

    static void prepareQuery(MappingSourceNode sourceNode, XMLPlannerEnvironment planEnv, QueryCommand rsQuery) 
        throws TeiidComponentException, QueryResolverException {
        
        Collection<GroupSymbol> externalGroups = getExternalGroups(sourceNode);
        
        rsQuery.setExternalGroupContexts(new GroupContext(null, externalGroups));
        
		QueryResolver.resolveCommand(rsQuery, planEnv.getGlobalMetadata());
    }
    
    private static Collection<GroupSymbol> getExternalGroups(MappingSourceNode sourceNode) {
        Collection<GroupSymbol> externalGroups = new HashSet<GroupSymbol>();

        MappingSourceNode parentSource = sourceNode.getParentSourceNode();
        while (parentSource != null) {
            externalGroups.add(new GroupSymbol(parentSource.getActualResultSetName()));
            parentSource = parentSource.getParentSourceNode();
        }
        return externalGroups;
    }
    
       
    /**
     * The Criteria Source nodes are source nodes underneath the context Node.  
     */
    private static boolean getResultSets(MappingSourceNode contextNode, Set<MappingSourceNode> criteriaSourceNodes, LinkedHashSet<MappingSourceNode> allResultSets)  {
        
        boolean singleParentage = true;

        for (Iterator<MappingSourceNode> i = criteriaSourceNodes.iterator(); i.hasNext();) {
            MappingSourceNode node = i.next();

            List<MappingSourceNode> rsStack = getResultSetStack(contextNode, node);
            
            if (allResultSets.containsAll(rsStack)) {
                continue;
            }
            if (!rsStack.containsAll(allResultSets)) {
                singleParentage = false;
            }
            allResultSets.addAll(rsStack);
        }
        
        return singleParentage;
    }
    
    private static LinkedList<MappingSourceNode> getResultSetStack(MappingSourceNode contextNode, MappingBaseNode node) {
        LinkedList<MappingSourceNode> rsStack = new LinkedList<MappingSourceNode>();
        
        while (node != null && node != contextNode) {
            if (node instanceof MappingSourceNode) {
                rsStack.add(0, (MappingSourceNode)node);
            }
            node = node.getParentNode();
        }
        return rsStack;
    }
    
    private static void planQueryWithCriteria(MappingSourceNode contextNode, XMLPlannerEnvironment planEnv) 
        throws QueryPlannerException, TeiidComponentException, QueryMetadataException, QueryResolverException {
        
        Map symbolMap = new HashMap();
        
        ResultSetInfo rsInfo = contextNode.getResultSetInfo();
        
        // this list of all the source nodes below the context, which are directly ro indirectly 
        // involved in the criteria
        LinkedHashSet<MappingSourceNode> resultSets = new LinkedHashSet<MappingSourceNode>();
        
        boolean singleParentage = getResultSets(contextNode, rsInfo.getCriteriaResultSets(), resultSets);
        
        Query contextQuery = null;
        
        if (rsInfo.isCriteriaRaised()) {
            contextQuery = (Query)QueryUtil.getQueryFromQueryNode(rsInfo.getResultSetName(), planEnv);
            String inlineViewName = planEnv.getAliasName(rsInfo.getResultSetName());
            updateSymbolMap(symbolMap, rsInfo.getResultSetName(), inlineViewName, planEnv.getGlobalMetadata());
        } else {
            contextQuery = (Query)rsInfo.getCommand();
        }
        
        Query currentQuery = contextQuery;
        
        for (MappingSourceNode rsNode : resultSets) {
            ResultSetInfo childRsInfo = rsNode.getResultSetInfo();
            
            QueryNode planNode = QueryUtil.getQueryNode(childRsInfo.getResultSetName(), planEnv.getGlobalMetadata());    
            Command command = QueryUtil.getQuery(childRsInfo.getResultSetName(), planNode, planEnv);
            
            String inlineViewName = planEnv.getAliasName(childRsInfo.getResultSetName());
            
            updateSymbolMap(symbolMap, childRsInfo.getResultSetName(), inlineViewName, planEnv.getGlobalMetadata());
            
            // check if the criteria has been raised, if it is then we can update this as a join.
            if (!rsInfo.isCritNullDependent() && childRsInfo.hasInputSet() && childRsInfo.isCriteriaRaised()) {
                Query transformationQuery = (Query) command;
                SubqueryFromClause sfc = (SubqueryFromClause)transformationQuery.getFrom().getClauses().get(0);
                
                Criteria joinCriteria = ((Query)childRsInfo.getCommand()).getCriteria();
                
                joinCriteria = (Criteria)joinCriteria.clone();
                
                //update the from clause
                FromClause clause = currentQuery.getFrom().getClauses().remove(0);
                
                JoinPredicate join = new JoinPredicate(clause, sfc, JoinType.JOIN_LEFT_OUTER, Criteria.separateCriteriaByAnd(joinCriteria));
                
                currentQuery.getFrom().addClause(join);
                
                currentQuery.getSelect().setDistinct(true);
                
                continue;
            }
            
            if (!singleParentage) {
                throw new QueryPlannerException(QueryPlugin.Util.getString("XMLQueryPlanner.cannot_plan", rsInfo.getCriteria())); //$NON-NLS-1$
            }
            
            Query subQuery = QueryUtil.wrapQuery(new SubqueryFromClause(inlineViewName, command), inlineViewName);

            currentQuery.setCriteria(Criteria.combineCriteria(currentQuery.getCriteria(), new ExistsCriteria(subQuery)));
            
            currentQuery = subQuery; 
        }
        
        Criteria userCrit = (Criteria)rsInfo.getCriteria().clone();
        
        currentQuery.setCriteria(Criteria.combineCriteria(currentQuery.getCriteria(), userCrit));
        
        StaticSymbolMappingVisitor.mapSymbols(contextQuery, symbolMap);
        
        if (rsInfo.isCriteriaRaised()) {
            //if allowing ancestor bindings, we need to update the bindings for the query node...
            prepareQuery(contextNode, planEnv, contextQuery);
            QueryUtil.rewriteQuery(contextQuery, planEnv.getGlobalMetadata(), planEnv.context);

            //selectively replace correlated references with their actual element symbols
            List<Reference> bindings = QueryUtil.getReferences(contextQuery);
            
            QueryNode modifiedNode = new QueryNode(null);
            modifiedNode.setCommand(contextQuery);
            
            for (Iterator<Reference> i = bindings.iterator(); i.hasNext();) {
                Reference ref = i.next();
                modifiedNode.addBinding(ref.getExpression().toString());
            }
            
            GroupSymbol groupSymbol = QueryUtil.createResolvedGroup(rsInfo.getResultSetName(), planEnv.getGlobalMetadata());
            planEnv.addQueryNodeToMetadata(groupSymbol.getMetadataID(), modifiedNode);
        } 
        
        for (Criteria crit : PredicateCollectorVisitor.getPredicates(userCrit)) {
        	handleXmlSubqueries(planEnv, crit);
        }
    }

	private static void handleXmlSubqueries(XMLPlannerEnvironment planEnv,
			Criteria userCrit) throws QueryPlannerException {
		if (!(userCrit instanceof SubqueryContainer<?>)) {
			return;
		}
    	SubqueryContainer<?> subquery = (SubqueryContainer<?>)userCrit;
    	if (!(subquery.getCommand() instanceof Query)) {
    		return;
    	}
		Query q = (Query)subquery.getCommand();
		if (q.getFrom() == null || q.getCriteria() == null) {
			return;
		}
		List<GroupSymbol> groups = q.getFrom().getGroups();
		if (groups.size() != 1) {
			return;
		}
		final GroupSymbol gs = groups.get(0);
		LinkedHashSet<GroupSymbol> allGroups = new LinkedHashSet<GroupSymbol>();
		allGroups.add(gs);
		//TODO: this group should have been marked as xml, or could attempt this step prior to place user criteria
		if (planEnv.getGlobalMetadata().getMetadataStore().getTempGroupID(gs.getNonCorrelationName().toUpperCase()) == null) {
			return;
		}
		MappingSourceNode parentMsn = findMappingSourceNode(planEnv, gs);
		for (Criteria crit : PredicateCollectorVisitor.getPredicates(q.getCriteria())) {
			Collection<ElementSymbol> elems = ElementCollectorVisitor.getElements(crit, false);
			Collection<GroupSymbol> critGroups = new LinkedList<GroupSymbol>();
			for (ElementSymbol elementSymbol : elems) {
				if (!elementSymbol.isExternalReference()) {
					critGroups.add(elementSymbol.getGroupSymbol());
				}
			}
			for (GroupSymbol groupSymbol : critGroups) {
				if (allGroups.contains(groupSymbol)) {
					continue;
				}
				MappingSourceNode childMsn = findMappingSourceNode(planEnv, groupSymbol);
				while (childMsn != parentMsn) {
					if (childMsn == null) {
						throw new QueryPlannerException(QueryPlugin.Util.getString("XMLQueryPlanner.invalid_relationship", crit, parentMsn)); //$NON-NLS-1$
					}
					if (!childMsn.getResultSetInfo().isCriteriaRaised()) {
						throw new QueryPlannerException(QueryPlugin.Util.getString("XMLQueryPlanner.non_simple_relationship", crit, childMsn)); //$NON-NLS-1$
					}
					Query parentQuery = (Query)childMsn.getResultSetInfo().getCommand();
					if (parentQuery.getCriteria() != null 
							&& allGroups.addAll(GroupsUsedByElementsVisitor.getGroups(parentQuery.getCriteria()))) {
						q.setCriteria(Criteria.combineCriteria(q.getCriteria(), (Criteria) parentQuery.getCriteria().clone()));
					}
					childMsn = childMsn.getParentSourceNode();
				}
			}
			q.getFrom().getClauses().clear();
			for (GroupSymbol groupSymbol : allGroups) {
				q.getFrom().addClause(new UnaryFromClause(groupSymbol));
			}
			handleXmlSubqueries(planEnv, crit);
		}
	}

	private static MappingSourceNode findMappingSourceNode(
			XMLPlannerEnvironment planEnv, final GroupSymbol gs) {
		MappingSourceNodeFinder finder = new MappingSourceNodeFinder(gs);
		planEnv.mappingDoc.acceptVisitor(new Navigator(true, finder));
		return finder.msn;
	}

    static void updateSymbolMap(Map symbolMap, String oldGroup, final String newGroup, QueryMetadataInterface metadata) 
        throws QueryResolverException,QueryMetadataException,TeiidComponentException {
        
        GroupSymbol oldGroupSymbol = new GroupSymbol(oldGroup);
        ResolverUtil.resolveGroup(oldGroupSymbol, metadata);
        
        HashSet<ElementSymbol> projectedElements = new HashSet<ElementSymbol>(ResolverUtil.resolveElementsInGroup(oldGroupSymbol, metadata));
        
        symbolMap.putAll(QueryUtil.createSymbolMap(oldGroupSymbol, newGroup, projectedElements));
    }
    
    /**
     * Currently any virtual/physical table can be planned as a staged table. A Staged
     * table only means that is has been preped to load the data into a temp table; when the other
     * transformations use this staged table, they will be redirected to use the temp table instead.
     * however note that it is still up to the plan to make sure the temp table is loaded.
     * @param groupName
     * @param planEnv
     * @return {@link GroupSymbol} the temptable which has been planned.
     * @throws QueryResolverException 
     */
    static void planStagingTable(String groupName, XMLPlannerEnvironment planEnv) 
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException, QueryResolverException {

        ResultSetInfo rsInfo = planEnv.getStagingTableResultsInfo(groupName);
        
        FromClause fromClause = new UnaryFromClause(new GroupSymbol(groupName));
        Query query = QueryUtil.wrapQuery(fromClause, groupName);
        if (rsInfo.getCriteria() != null) {
            query.setCriteria(rsInfo.getCriteria());
        }
        planStagaingQuery(false, groupName, groupName, query, planEnv);
    }
    /**
     * This method takes given query and adds the "into" symbol to query and resoves it
     * and registers it with planner env as the staging table. Also, builds a unload query
     * to unload the staging table.
     * @throws QueryResolverException 
     */
    static boolean planStagaingQuery(boolean implicit, String srcGroupName, String stageGroupName, Query query, XMLPlannerEnvironment planEnv) 
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException, QueryResolverException {

        GroupSymbol srcGroup = QueryUtil.createResolvedGroup(srcGroupName, planEnv.getGlobalMetadata());
        
        String intoGroupName = getTempTableName(stageGroupName);
        GroupSymbol intoGroupSymbol = new GroupSymbol(intoGroupName); 
                
        query.setInto(new Into(intoGroupSymbol));
        
        QueryResolver.resolveCommand(query, planEnv.getGlobalMetadata());
        
        Command cmd = QueryUtil.rewriteQuery(query, planEnv.getGlobalMetadata(), planEnv.context);
                
        ProcessorPlan plan = null;
        
        boolean debug = planEnv.analysisRecord.recordDebug();
        
        if (debug) {
            planEnv.analysisRecord.println("Attempting to create plan for staging table " + srcGroupName); //$NON-NLS-1$
        }
        
        try {
            // register with env
            plan = optimizePlan(cmd, planEnv);
        } catch (QueryPlannerException e) {
            if (implicit) {
                if (debug) {
                    planEnv.analysisRecord.println("Failed to create plan for staging table " + srcGroupName + " due to " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
                }
                return false;
            } 
            throw e;
        }
        
        int cardinality = QueryMetadataInterface.UNKNOWN_CARDINALITY;
        
        if (plan instanceof RelationalPlan) {
            RelationalPlan relationalPlan = (RelationalPlan)plan;
            RelationalNode root = relationalPlan.getRootNode();
            //since the root will be a project into node, get the cost from its child
            if (root.getChildren()[0] != null) {
                root = root.getChildren()[0];
            }
            Number planCardinality = root.getEstimateNodeCardinality();
            
            if (planCardinality == null || planCardinality.floatValue() == NewCalculateCostUtil.UNKNOWN_VALUE) {
                //don't stage unknown cost without criteria
                if (implicit && query.getCriteria() == null) {
                    return false;
                }
            } else if (planCardinality.floatValue() < planEnv.context.getProcessorBatchSize()) {
                //the staging table seems small    
                cardinality = planCardinality.intValue();
            } else if (implicit) {
                return false;
            }
        }
        
        // since this was staging table; this adds some temp metadata to the query node; extract
        // that metadata and inject into global metadata store for rest of the queries to use.
        Map tempMetadata = query.getTemporaryMetadata();
        if (tempMetadata != null && !tempMetadata.isEmpty()) {
            planEnv.addToGlobalMetadata(tempMetadata);
        }
        
        ResultSetInfo rsInfo = planEnv.getStagingTableResultsInfo(stageGroupName);
        rsInfo.setCommand(cmd);
        rsInfo.setPlan(plan);
        
        //set the cardinality on the temp group.
        TempMetadataID intoGroupID = (TempMetadataID)intoGroupSymbol.getMetadataID();
        intoGroupID.setCardinality(cardinality);
        
        // add the materialization hook for the staged table to original one.
        //GroupSymbol groupSymbol = (GroupSymbol)query.getFrom().getGroups().get(0);
        planEnv.addStagingTable(srcGroup.getMetadataID(), intoGroupID);
        
        // plan the unload of the staging table
        String unloadName = planEnv.unLoadResultName(stageGroupName);
        ResultSetInfo rsUnloadInfo = planEnv.getStagingTableResultsInfo(unloadName);
        Command command = wrapStagingTableUnloadQuery(intoGroupSymbol);
        QueryResolver.resolveCommand(command, planEnv.getGlobalMetadata());
        command = QueryUtil.rewriteQuery(command, planEnv.getGlobalMetadata(), planEnv.context);
        
        plan = optimizePlan(command, planEnv);
        rsUnloadInfo.setCommand(command);
        rsUnloadInfo.setPlan(plan);
        
        return true;
    }

	static String getTempTableName(String stageGroupName) {
		String intoGroupName =  "#"+stageGroupName.replace('.', '_'); //$NON-NLS-1$
		return intoGroupName;
	}
        
    /**
     * This builds a command in the following form; If staging table name is "FOO"
     * the command built is "Delete FROM #FOO"
     */    
    private static Command wrapStagingTableUnloadQuery(GroupSymbol intoGroupSymbol) {
        Drop drop = new Drop();
        drop.setTable(intoGroupSymbol);
        return drop;
    }    
}
