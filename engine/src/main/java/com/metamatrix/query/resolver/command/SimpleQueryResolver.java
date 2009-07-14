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

package com.metamatrix.query.resolver.command;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.api.exception.query.UnresolvedSymbolDescription;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.dqp.message.ParameterInfo;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.analysis.QueryAnnotation;
import com.metamatrix.query.mapping.relational.QueryNode;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.StoredProcedureInfo;
import com.metamatrix.query.metadata.SupportConstants;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.resolver.CommandResolver;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.resolver.util.BindVariableVisitor;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.resolver.util.ResolverVisitor;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.ExistsCriteria;
import com.metamatrix.query.sql.lang.From;
import com.metamatrix.query.sql.lang.Into;
import com.metamatrix.query.sql.lang.JoinPredicate;
import com.metamatrix.query.sql.lang.Option;
import com.metamatrix.query.sql.lang.OrderBy;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.SPParameter;
import com.metamatrix.query.sql.lang.Select;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.lang.SubqueryCompareCriteria;
import com.metamatrix.query.sql.lang.SubqueryContainer;
import com.metamatrix.query.sql.lang.SubqueryFromClause;
import com.metamatrix.query.sql.lang.SubquerySetCriteria;
import com.metamatrix.query.sql.lang.UnaryFromClause;
import com.metamatrix.query.sql.navigator.PostOrderNavigator;
import com.metamatrix.query.sql.symbol.AliasSymbol;
import com.metamatrix.query.sql.symbol.AllInGroupSymbol;
import com.metamatrix.query.sql.symbol.AllSymbol;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.ExpressionSymbol;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.symbol.ScalarSubquery;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.util.ErrorMessageKeys;
import com.metamatrix.query.util.LogConstants;

public class SimpleQueryResolver implements CommandResolver {

    private static final String ALL_IN_GROUP_SUFFIX = ".*"; //$NON-NLS-1$

    private static Command resolveVirtualGroup(GroupSymbol virtualGroup, Command parentCommand, QueryMetadataInterface metadata, AnalysisRecord analysis)
    throws QueryMetadataException, QueryResolverException, MetaMatrixComponentException {
        QueryNode qnode = null;
        
        Object metadataID = virtualGroup.getMetadataID();
        boolean isSelectInto = ((Query)parentCommand).getInto() != null;
        boolean isMaterializedViewLoad = false;
        boolean noCache = false;
        boolean cacheCommand = false;
        boolean isMaterializedGroup = metadata.hasMaterialization(metadataID);
        if( isMaterializedGroup) {
            if(isSelectInto) {
                //Case 2945: Only bypass Mat View logic if this is an explicit load into 
                //the Matierialzed View.
                final Object intoGrpID = ((Query)parentCommand).getInto().getGroup().getMetadataID();
                final Object matID = metadata.getMaterialization(metadataID);
                final Object matSTID =  metadata.getMaterializationStage(metadataID);
                if(matID != null) {
                    isMaterializedViewLoad = matID.equals(intoGrpID);
                }
                
                if(matSTID != null && !isMaterializedViewLoad) {
                    isMaterializedViewLoad = matSTID.equals(intoGrpID);
                }
            }

            Option option  = parentCommand.getOption();
            noCache = isNoCacheGroup(metadata, metadataID, option);
        	if(noCache){
        		//not use cache
        		qnode = metadata.getVirtualPlan(metadataID);
        		String matTableName = metadata.getFullName(metadata.getMaterialization(metadataID));
        		recordMaterializedTableNotUsedAnnotation(virtualGroup, analysis, matTableName);
        	}else{
	            if(!isMaterializedViewLoad) {           	
	                // Default query for a materialized group - go to cached table
	                String groupName = metadata.getFullName(metadataID);
	                String matTableName = metadata.getFullName(metadata.getMaterialization(metadataID));
	                qnode = new QueryNode(groupName, "SELECT * FROM " + matTableName); //$NON-NLS-1$
	                
	                recordMaterializationTableAnnotation(virtualGroup, analysis, matTableName);                
	            } else {
	                // Loading due to SELECT INTO - query the primary transformation
	                qnode = metadata.getVirtualPlan(metadataID);
	
	                recordLoadingMaterializationTableAnnotation(virtualGroup, analysis);                
	            }
        	}
        } else {
            if (metadata.isXMLGroup(virtualGroup.getMetadataID())) {
                throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0003, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0003));
            }
            cacheCommand = true;
            Command command = (Command)metadata.getFromMetadataCache(virtualGroup.getMetadataID(), "transformation/select"); //$NON-NLS-1$
            if (command != null) {
            	command = (Command)command.clone();
            	command.setVirtualGroup(virtualGroup);
            	return command;
            }
            // Not a materialized view - query the primary transformation
            qnode = metadata.getVirtualPlan(metadataID);            
        }
        
        Command subCommand = convertToSubquery(qnode, noCache, metadata);
        subCommand.setVirtualGroup(virtualGroup);
        QueryResolver.resolveCommand(subCommand, Collections.EMPTY_MAP, true, metadata, analysis);
        if (cacheCommand) {
        	metadata.addToMetadataCache(virtualGroup.getMetadataID(), "transformation/select", subCommand.clone()); //$NON-NLS-1$
        }        
        return subCommand;
    }

    /** 
     * @param metadata
     * @param metadataID
     * @param noCache
     * @param option
     * @return
     * @throws QueryMetadataException
     * @throws MetaMatrixComponentException
     */
    public static boolean isNoCacheGroup(QueryMetadataInterface metadata,
                                          Object metadataID,
                                          Option option) throws QueryMetadataException,
                                                        MetaMatrixComponentException {
        if(option == null){
            return false;
        }
    	if(option.isNoCache() && (option.getNoCacheGroups() == null || option.getNoCacheGroups().isEmpty())){
    		//only OPTION NOCACHE, no group specified
    		return true;
    	}       
        if(option.getNoCacheGroups() != null){
            for(int i=0; i< option.getNoCacheGroups().size(); i++){
                String groupName = (String)option.getNoCacheGroups().get(i);
                try {
                    Object noCacheGroupID = metadata.getGroupID(groupName);
                    if(metadataID.equals(noCacheGroupID)){
                        return true;
                    }
                } catch (QueryMetadataException e) {
                    //log that an unknown groups was used in the no cache
                    LogManager.logWarning(LogConstants.CTX_QUERY_RESOLVER, e, QueryPlugin.Util.getString("SimpleQueryResolver.unknown_group_in_nocache", groupName)); //$NON-NLS-1$
                }
            }
    	}
        return false;
    }
    
    /**
	 * @param virtualGroup
	 * @param analysis
	 */
	private static void recordMaterializedTableNotUsedAnnotation(GroupSymbol virtualGroup, AnalysisRecord analysis, String matTableName) {
        if ( analysis.recordAnnotations() ) {
            Object[] params = new Object[] {virtualGroup, matTableName};
            QueryAnnotation annotation = new QueryAnnotation(QueryAnnotation.MATERIALIZED_VIEW, 
                                                         QueryPlugin.Util.getString("SimpleQueryResolver.materialized_table_not_used", params),  //$NON-NLS-1$
                                                         null, 
                                                         QueryAnnotation.LOW);
            analysis.addAnnotation(annotation);
        }
	}

	/** 
     * @param virtualGroup
     * @param analysis
     * @param matTableName
     * @since 4.2
     */
    private static void recordMaterializationTableAnnotation(GroupSymbol virtualGroup,
                                                      AnalysisRecord analysis,
                                                      String matTableName) {
        if ( analysis.recordAnnotations() ) {
            Object[] params = new Object[] {virtualGroup, matTableName};
            QueryAnnotation annotation = new QueryAnnotation(QueryAnnotation.MATERIALIZED_VIEW, 
                                                         QueryPlugin.Util.getString("SimpleQueryResolver.Query_was_redirected_to_Mat_table", params),  //$NON-NLS-1$
                                                         null, 
                                                         QueryAnnotation.LOW);
            analysis.addAnnotation(annotation);
        }
    }

    /** 
     * @param virtualGroup
     * @param analysis
     * @param matTableName
     * @since 4.2
     */
    private static void recordLoadingMaterializationTableAnnotation(GroupSymbol virtualGroup,
                                                      AnalysisRecord analysis) {
        if ( analysis.recordAnnotations() ) {
            Object[] params = new Object[] {virtualGroup};
            QueryAnnotation annotation = new QueryAnnotation(QueryAnnotation.MATERIALIZED_VIEW, 
                                                         QueryPlugin.Util.getString("SimpleQueryResolver.Loading_materialized_table", params),  //$NON-NLS-1$
                                                         null, 
                                                         QueryAnnotation.LOW);
            analysis.addAnnotation(annotation);
        }
    }

    private static Command convertToSubquery(QueryNode qnode, boolean nocache, QueryMetadataInterface metadata)
    throws QueryResolverException, MetaMatrixComponentException {

        // Parse this node's command
        Command command = qnode.getCommand();
        
        if (command == null) {
            try {
                command = QueryParser.getQueryParser().parseCommand(qnode.getQuery());
            } catch(QueryParserException e) {
                throw new QueryResolverException(e, ErrorMessageKeys.RESOLVER_0011, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0011, qnode.getGroupName()));
            }
            
            //Handle bindings and references
            List bindings = qnode.getBindings();
            if (bindings != null){
                BindVariableVisitor.bindReferences(command, bindings, metadata);
            }
        }
        
        if (nocache) {
            Option option = command.getOption();
            if (option == null) {
                option = new Option();
                command.setOption(option);
            }
            option.setNoCache(true);
            if (option.getNoCacheGroups() != null) {
                option.getNoCacheGroups().clear();
            }
        }

        return command;
    }

    /** 
     * @see com.metamatrix.query.resolver.CommandResolver#resolveCommand(com.metamatrix.query.sql.lang.Command, boolean, com.metamatrix.query.metadata.TempMetadataAdapter, com.metamatrix.query.analysis.AnalysisRecord, boolean)
     */
    public void resolveCommand(Command command, boolean useMetadataCommands, TempMetadataAdapter metadata, AnalysisRecord analysis, boolean resolveNullLiterals)
        throws QueryMetadataException, QueryResolverException, MetaMatrixComponentException {

        Query query = (Query) command;
        
        try {
            QueryResolverVisitor qrv = new QueryResolverVisitor(query, metadata, useMetadataCommands, analysis);
            qrv.visit(query);
            ResolverVisitor visitor = (ResolverVisitor)qrv.getVisitor();
			visitor.throwException(true);
        } catch (MetaMatrixRuntimeException e) {
            if (e.getChild() instanceof QueryMetadataException) {
                throw (QueryMetadataException)e.getChild();
            }
            if (e.getChild() instanceof QueryResolverException) {
                throw (QueryResolverException)e.getChild();
            }
            if (e.getChild() instanceof MetaMatrixComponentException) {
                throw (MetaMatrixComponentException)e.getChild();
            }
            throw e;
        }
                                       
        if (query.getLimit() != null) {
            ResolverUtil.resolveLimit(query.getLimit());
        }
        
        List symbols = query.getSelect().getProjectedSymbols();
        
        if (query.getInto() != null) {
            GroupSymbol symbol = query.getInto().getGroup();
            ResolverUtil.resolveImplicitTempGroup(metadata, symbol, symbols);
        } else if (resolveNullLiterals) {
            ResolverUtil.resolveNullLiterals(symbols);
        }
    }

    private static GroupSymbol resolveAllInGroup(AllInGroupSymbol allInGroupSymbol, Set groups, QueryMetadataInterface metadata) throws QueryResolverException, QueryMetadataException, MetaMatrixComponentException {       
        String name = allInGroupSymbol.getName();
        int index = name.lastIndexOf(ALL_IN_GROUP_SUFFIX);
        String groupAlias = name.substring(0, index);
        List groupSymbols = ResolverUtil.findMatchingGroups(groupAlias.toUpperCase(), groups, metadata);
        if(groupSymbols.isEmpty() || groupSymbols.size() > 1) {
            String msg = QueryPlugin.Util.getString(groupSymbols.isEmpty()?ErrorMessageKeys.RESOLVER_0047:"SimpleQueryResolver.ambiguous_all_in_group", allInGroupSymbol);  //$NON-NLS-1$
            QueryResolverException qre = new QueryResolverException(msg);
            qre.addUnresolvedSymbol(new UnresolvedSymbolDescription(allInGroupSymbol.toString(), msg));
            throw qre;
        }

        return (GroupSymbol)groupSymbols.get(0);
    }
    
    public static class QueryResolverVisitor extends PostOrderNavigator {

        private LinkedHashSet<GroupSymbol> currentGroups = new LinkedHashSet<GroupSymbol>();
        private List<GroupSymbol> discoveredGroups = new LinkedList<GroupSymbol>();
        private TempMetadataAdapter metadata;
        private boolean expandCommand;
        private Query query;
        private AnalysisRecord analysis;
        
        public QueryResolverVisitor(Query query, TempMetadataAdapter metadata, boolean expandCommand, AnalysisRecord record) {
            super(new ResolverVisitor(metadata, null, query.getExternalGroupContexts()));
            ResolverVisitor visitor = (ResolverVisitor)getVisitor();
            visitor.setGroups(currentGroups);
            this.query = query;
            this.metadata = metadata;
            this.expandCommand = expandCommand;
            this.analysis = record;
        }
        
        protected void postVisitVisitor(LanguageObject obj) {
            super.postVisitVisitor(obj);
            ResolverVisitor visitor = (ResolverVisitor)getVisitor();
            try {
				visitor.throwException(false);
			} catch (QueryResolverException e) {
				throw new MetaMatrixRuntimeException(e);
			} catch (MetaMatrixComponentException e) {
				throw new MetaMatrixRuntimeException(e);
			}
        }
                
        /**
         * Resolving a Query requires a special ordering
         * 
         * Note that into is actually first to handle mat view logic
         */
        public void visit(Query obj) {
            visitNode(obj.getInto());
            visitNode(obj.getFrom());
            visitNode(obj.getCriteria());
            visitNode(obj.getGroupBy());
            visitNode(obj.getHaving());
            visitNode(obj.getSelect());        
            visitNode(obj.getOrderBy());
        }
        
        public void visit(GroupSymbol obj) {
            try {
                ResolverUtil.resolveGroup(obj, metadata);
            } catch (QueryResolverException err) {
                throw new MetaMatrixRuntimeException(err);
            } catch (MetaMatrixComponentException err) {
                throw new MetaMatrixRuntimeException(err);
            }
        }
                        
        private void resolveSubQuery(SubqueryContainer obj) {
            Command command = obj.getCommand();
            
            QueryResolver.setChildMetadata(command, query);
            command.pushNewResolvingContext(this.currentGroups);
            
            try {
                QueryResolver.resolveCommand(command, Collections.EMPTY_MAP, expandCommand, metadata.getMetadata(), analysis, false);
            } catch (QueryResolverException err) {
                throw new MetaMatrixRuntimeException(err);
            } catch (MetaMatrixComponentException err) {
                throw new MetaMatrixRuntimeException(err);
            }
        }
        
        public void visit(AllSymbol obj) {
            try {
                List elementSymbols = new ArrayList();
                Iterator groupIter = currentGroups.iterator();
                while(groupIter.hasNext()){
                    GroupSymbol group = (GroupSymbol)groupIter.next();
    
                    List elements = resolveSelectableElements(group);
    
                    elementSymbols.addAll(elements);
                }
                obj.setElementSymbols(elementSymbols);
            } catch (MetaMatrixComponentException err) {
                throw new MetaMatrixRuntimeException(err);
            } 
        }

        private List resolveSelectableElements(GroupSymbol group) throws QueryMetadataException,
                                                                 MetaMatrixComponentException {
            List elements = ResolverUtil.resolveElementsInGroup(group, metadata);
            
            List result = new ArrayList(elements.size());
   
            // Look for elements that are not selectable and remove them
            Iterator elementIter = elements.iterator();
            while(elementIter.hasNext()) {
                ElementSymbol element = (ElementSymbol) elementIter.next();
                if(metadata.elementSupports(element.getMetadataID(), SupportConstants.Element.SELECT)) {
                    element = (ElementSymbol)element.clone();
                    element.setGroupSymbol(group);
                	result.add(element);
                }
            }
            return result;
        }
        
        public void visit(AllInGroupSymbol obj) {
            // Determine group that this symbol is for
            try {
                GroupSymbol group = resolveAllInGroup(obj, currentGroups, metadata);
                
                List elements = resolveSelectableElements(group);
                
                obj.setElementSymbols(elements);
            } catch (QueryResolverException err) {
                throw new MetaMatrixRuntimeException(err);
            } catch (MetaMatrixComponentException err) {
                throw new MetaMatrixRuntimeException(err);
            } 
        }
        
        public void visit(ScalarSubquery obj) {
            resolveSubQuery(obj);
            
            Collection projSymbols = obj.getCommand().getProjectedSymbols();

            //Scalar subquery should have one projected symbol (query with one expression
            //in SELECT or stored procedure execution that returns a single value).
            if(projSymbols.size() != 1) {
                QueryResolverException qre = new QueryResolverException(QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0032, obj));
                throw new MetaMatrixRuntimeException(qre);
            }
        }
        
        public void visit(ExistsCriteria obj) {
            resolveSubQuery(obj);
        }
        
        public void visit(SubqueryCompareCriteria obj) {
            visitNode(obj.getLeftExpression());
            resolveSubQuery(obj);
            postVisitVisitor(obj);
        }
        
        public void visit(SubquerySetCriteria obj) {
            visitNode(obj.getExpression());
            resolveSubQuery(obj);
            postVisitVisitor(obj);
        }
        
        public void visit(SubqueryFromClause obj) {
            resolveSubQuery(obj);
            this.discoveredGroups.add(obj.getGroupSymbol());
            try {
                ResolverUtil.addTempGroup(metadata, obj.getGroupSymbol(), obj.getCommand().getProjectedSymbols(), false);
            } catch (QueryResolverException err) {
                throw new MetaMatrixRuntimeException(err);
            }
            obj.getGroupSymbol().setMetadataID(metadata.getMetadataStore().getTempGroupID(obj.getGroupSymbol().getName())); 
        }
                        
        public void visit(UnaryFromClause obj) {
            GroupSymbol group = obj.getGroup();
            visitNode(group);
            this.discoveredGroups.add(group);
            
            try {
                if (expandCommand
                    && !group.isTempGroupSymbol()
                    && !group.isProcedure()
                    && (!(group.getMetadataID() instanceof TempMetadataID) || metadata.getVirtualPlan(group.getMetadataID()) != null)
                    && (metadata.isVirtualGroup(group.getMetadataID()))) {
                    
                    Command command = resolveVirtualGroup(group, query, metadata.getMetadata(), analysis);                    
                    obj.setExpandedCommand(command);
                } else if (group.isProcedure()) {
                    //"relational" select of a virtual procedure
                    String fullName = metadata.getFullName(group.getMetadataID());
                    String queryName = group.getName();
                    
                    StoredProcedureInfo storedProcedureInfo = metadata.getStoredProcedureInfoForProcedure(fullName);

                    StoredProcedure storedProcedureCommand = new StoredProcedure();
                    storedProcedureCommand.setProcedureRelational(true);
                    storedProcedureCommand.setProcedureName(fullName);
                    
                    List metadataParams = storedProcedureInfo.getParameters();
                    
                    Query procQuery = new Query();
                    From from = new From();
                    from.addClause(new SubqueryFromClause("X", storedProcedureCommand)); //$NON-NLS-1$
                    procQuery.setFrom(from);
                    Select select = new Select();
                    select.addSymbol(new AllInGroupSymbol("X.*")); //$NON-NLS-1$
                    procQuery.setSelect(select);
                    
                    List accessPatternElementNames = new LinkedList();
                    
                    int paramIndex = 1;
                    
                    for(Iterator paramIter = metadataParams.iterator(); paramIter.hasNext();){
                        SPParameter metadataParameter  = (SPParameter)paramIter.next();
                        SPParameter clonedParam = (SPParameter)metadataParameter.clone();
                        if (clonedParam.getParameterType()==ParameterInfo.IN || metadataParameter.getParameterType()==ParameterInfo.INOUT) {
                            ElementSymbol paramSymbol = clonedParam.getParameterSymbol();
                            Reference ref = new Reference(paramSymbol);
                            clonedParam.setExpression(ref);
                            clonedParam.setIndex(paramIndex++);
                            storedProcedureCommand.setParameter(clonedParam);
                            
                            String aliasName = paramSymbol.getShortName();
                            
                            if (metadataParameter.getParameterType()==ParameterInfo.INOUT) {
                                aliasName += "_IN"; //$NON-NLS-1$
                            }
                            
                            SingleElementSymbol newSymbol = new AliasSymbol(aliasName, new ExpressionSymbol(paramSymbol.getShortName(), ref));
                            
                            select.addSymbol(newSymbol);
                            accessPatternElementNames.add(queryName + ElementSymbol.SEPARATOR + aliasName);
                        }
                    }
                    
                    QueryResolver.resolveCommand(procQuery, Collections.EMPTY_MAP, expandCommand, metadata.getMetadata(), analysis);
                    
                    List projectedSymbols = procQuery.getProjectedSymbols();
                    
                    HashSet foundNames = new HashSet();
                    
                    for (Iterator i = projectedSymbols.iterator(); i.hasNext();) {
                        SingleElementSymbol ses = (SingleElementSymbol)i.next();
                        if (!foundNames.add(ses.getShortCanonicalName())) {
                            throw new QueryResolverException(QueryPlugin.Util.getString("SimpleQueryResolver.Proc_Relational_Name_conflict", fullName)); //$NON-NLS-1$                            
                        }
                    }
                    
                    TempMetadataID id = metadata.getMetadataStore().getTempGroupID(queryName);

                    if (id == null) {
                        metadata.getMetadataStore().addTempGroup(queryName, projectedSymbols, true);
                        
                        id = metadata.getMetadataStore().getTempGroupID(queryName);
                        id.setOriginalMetadataID(storedProcedureCommand.getProcedureID());
                        List accessPatternIds = new LinkedList();
                        
                        for (Iterator i = accessPatternElementNames.iterator(); i.hasNext();) {
                            String name = (String)i.next();
                            accessPatternIds.add(metadata.getMetadataStore().getTempElementID(name));
                        }
                        
                        id.setAccessPatterns(Arrays.asList(new TempMetadataID("procedure access pattern", accessPatternIds))); //$NON-NLS-1$
                    }
                    
                    group.setMetadataID(id);
                    group.setProcedure(true);
                    procQuery.setVirtualGroup(group);
                    
                    if (expandCommand) {
                        obj.setExpandedCommand(procQuery);
                    }
                }
            } catch(QueryResolverException e) {
                throw new MetaMatrixRuntimeException(e);
            } catch(MetaMatrixComponentException e) {
                throw new MetaMatrixRuntimeException(e);                        
			}
        }
        
        public void visit(OrderBy obj) {
            try {
                ResolverUtil.resolveOrderBy(obj, new ArrayList(currentGroups), query.getSelect().getProjectedSymbols(), metadata, query.getGroupBy() == null && !query.getSelect().isDistinct());
            } catch(QueryResolverException e) {
                throw new MetaMatrixRuntimeException(e);
            } catch(MetaMatrixComponentException e) {
                throw new MetaMatrixRuntimeException(e);                        
            }
        }
        
        /** 
         * @see com.metamatrix.query.sql.navigator.PreOrPostOrderNavigator#visit(com.metamatrix.query.sql.lang.Into)
         */
        public void visit(Into obj) {
            if (!obj.getGroup().isImplicitTempGroupSymbol()) {
                super.visit(obj);
            }
        }

        public void visit(JoinPredicate obj) {
        	List<GroupSymbol> pendingDiscoveredGroups = new ArrayList<GroupSymbol>(discoveredGroups);
        	discoveredGroups.clear();
            visitNode(obj.getLeftClause());
            visitNode(obj.getRightClause());
            
            addDiscoveredGroups();
            discoveredGroups = pendingDiscoveredGroups;
            visitNodes(obj.getJoinCriteria());
        }

		private void addDiscoveredGroups() {
			for (GroupSymbol group : discoveredGroups) {
				if (!this.currentGroups.add(group)) {
	                String msg = QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0046, group.getName());
	                QueryResolverException qre = new QueryResolverException(ErrorMessageKeys.RESOLVER_0046, msg);
	                qre.addUnresolvedSymbol(new UnresolvedSymbolDescription(group.toString(), msg));
	                throw new MetaMatrixRuntimeException(qre);
	            }
			}
            discoveredGroups.clear();
		}
                
        public void visit(From obj) {
            assert currentGroups.isEmpty();
            super.visit(obj);
            addDiscoveredGroups();
        }
    }

}
