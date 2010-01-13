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
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.api.exception.query.UnresolvedSymbolDescription;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.dqp.message.ParameterInfo;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.StoredProcedureInfo;
import com.metamatrix.query.metadata.SupportConstants;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.resolver.CommandResolver;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.resolver.util.ResolverVisitor;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.ExistsCriteria;
import com.metamatrix.query.sql.lang.From;
import com.metamatrix.query.sql.lang.Into;
import com.metamatrix.query.sql.lang.JoinPredicate;
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

public class SimpleQueryResolver implements CommandResolver {

    private static final String ALL_IN_GROUP_SUFFIX = ".*"; //$NON-NLS-1$

    /** 
     * @see com.metamatrix.query.resolver.CommandResolver#resolveCommand(com.metamatrix.query.sql.lang.Command, com.metamatrix.query.metadata.TempMetadataAdapter, com.metamatrix.query.analysis.AnalysisRecord, boolean)
     */
    public void resolveCommand(Command command, TempMetadataAdapter metadata, AnalysisRecord analysis, boolean resolveNullLiterals)
        throws QueryMetadataException, QueryResolverException, MetaMatrixComponentException {

        Query query = (Query) command;
        
        try {
            QueryResolverVisitor qrv = new QueryResolverVisitor(query, metadata, analysis);
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
        
        if (query.getOrderBy() != null) {
        	ResolverUtil.resolveOrderBy(query.getOrderBy(), query, metadata);
        }
        
        List symbols = query.getSelect().getProjectedSymbols();
        
        if (query.getInto() != null) {
            GroupSymbol symbol = query.getInto().getGroup();
            ResolverUtil.resolveImplicitTempGroup(metadata, symbol, symbols);
        } else if (resolveNullLiterals) {
            ResolverUtil.resolveNullLiterals(symbols);
        }
    }

    private static GroupSymbol resolveAllInGroup(AllInGroupSymbol allInGroupSymbol, Set<GroupSymbol> groups, QueryMetadataInterface metadata) throws QueryResolverException, QueryMetadataException, MetaMatrixComponentException {       
        String name = allInGroupSymbol.getName();
        int index = name.lastIndexOf(ALL_IN_GROUP_SUFFIX);
        String groupAlias = name.substring(0, index);
        List<GroupSymbol> groupSymbols = ResolverUtil.findMatchingGroups(groupAlias.toUpperCase(), groups, metadata);
        if(groupSymbols.isEmpty() || groupSymbols.size() > 1) {
            String msg = QueryPlugin.Util.getString(groupSymbols.isEmpty()?ErrorMessageKeys.RESOLVER_0047:"SimpleQueryResolver.ambiguous_all_in_group", allInGroupSymbol);  //$NON-NLS-1$
            QueryResolverException qre = new QueryResolverException(msg);
            qre.addUnresolvedSymbol(new UnresolvedSymbolDescription(allInGroupSymbol.toString(), msg));
            throw qre;
        }

        return groupSymbols.get(0);
    }
    
    public static class QueryResolverVisitor extends PostOrderNavigator {

        private LinkedHashSet<GroupSymbol> currentGroups = new LinkedHashSet<GroupSymbol>();
        private List<GroupSymbol> discoveredGroups = new LinkedList<GroupSymbol>();
        private TempMetadataAdapter metadata;
        private Query query;
        private AnalysisRecord analysis;
        
        public QueryResolverVisitor(Query query, TempMetadataAdapter metadata, AnalysisRecord record) {
            super(new ResolverVisitor(metadata, null, query.getExternalGroupContexts()));
            ResolverVisitor visitor = (ResolverVisitor)getVisitor();
            visitor.setGroups(currentGroups);
            this.query = query;
            this.metadata = metadata;
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
         */
        public void visit(Query obj) {
            visitNode(obj.getInto());
            visitNode(obj.getFrom());
            visitNode(obj.getCriteria());
            visitNode(obj.getGroupBy());
            visitNode(obj.getHaving());
            visitNode(obj.getSelect());        
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
                QueryResolver.resolveCommand(command, Collections.EMPTY_MAP, metadata.getMetadata(), analysis, false);
            } catch (QueryResolverException err) {
                throw new MetaMatrixRuntimeException(err);
            } catch (MetaMatrixComponentException err) {
                throw new MetaMatrixRuntimeException(err);
            }
        }
        
        public void visit(AllSymbol obj) {
            try {
                List<ElementSymbol> elementSymbols = new ArrayList<ElementSymbol>();
                for (GroupSymbol group : currentGroups) {
                    elementSymbols.addAll(resolveSelectableElements(group));
                }
                obj.setElementSymbols(elementSymbols);
            } catch (MetaMatrixComponentException err) {
                throw new MetaMatrixRuntimeException(err);
            } 
        }

        private List<ElementSymbol> resolveSelectableElements(GroupSymbol group) throws QueryMetadataException,
                                                                 MetaMatrixComponentException {
            List<ElementSymbol> elements = ResolverUtil.resolveElementsInGroup(group, metadata);
            
            List<ElementSymbol> result = new ArrayList<ElementSymbol>(elements.size());
   
            // Look for elements that are not selectable and remove them
            for (ElementSymbol element : elements) {
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
                
                List<ElementSymbol> elements = resolveSelectableElements(group);
                
                obj.setElementSymbols(elements);
            } catch (QueryResolverException err) {
                throw new MetaMatrixRuntimeException(err);
            } catch (MetaMatrixComponentException err) {
                throw new MetaMatrixRuntimeException(err);
            } 
        }
        
        public void visit(ScalarSubquery obj) {
            resolveSubQuery(obj);
            
            Collection<SingleElementSymbol> projSymbols = obj.getCommand().getProjectedSymbols();

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
            try {
	            if (!group.isProcedure() && metadata.isXMLGroup(group.getMetadataID())) {
	                throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0003, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0003));
	            }
	            this.discoveredGroups.add(group);
	            if (group.isProcedure()) {
	                createProcRelational(obj);
	            }
            } catch(QueryResolverException e) {
                throw new MetaMatrixRuntimeException(e);
            } catch(MetaMatrixComponentException e) {
                throw new MetaMatrixRuntimeException(e);                        
			}
        }

		private void createProcRelational(UnaryFromClause obj)
				throws MetaMatrixComponentException, QueryMetadataException,
				QueryResolverException {
			GroupSymbol group = obj.getGroup();
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
			
			List<String> accessPatternElementNames = new LinkedList<String>();
			
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
			
			QueryResolver.resolveCommand(procQuery, Collections.EMPTY_MAP, metadata.getMetadata(), analysis);
			
			List projectedSymbols = procQuery.getProjectedSymbols();
			
			HashSet<String> foundNames = new HashSet<String>();
			
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
			    if (!accessPatternElementNames.isEmpty()) {
				    List<TempMetadataID> accessPatternIds = new LinkedList<TempMetadataID>();
				    
				    for (String name : accessPatternElementNames) {
				        accessPatternIds.add(metadata.getMetadataStore().getTempElementID(name));
				    }
				    
				    id.setAccessPatterns(Arrays.asList(new TempMetadataID("procedure access pattern", accessPatternIds))); //$NON-NLS-1$
			    }
			}
			
			group.setMetadataID(id);
			
		    obj.setExpandedCommand(procQuery);
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
