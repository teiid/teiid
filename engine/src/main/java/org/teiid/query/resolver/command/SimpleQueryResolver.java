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

package org.teiid.query.resolver.command;

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

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.UnresolvedSymbolDescription;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.StoredProcedureInfo;
import org.teiid.query.metadata.SupportConstants;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.resolver.CommandResolver;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.ArrayTable;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.Into;
import org.teiid.query.sql.lang.JoinPredicate;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.lang.SubqueryFromClause;
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.lang.TableFunctionReference;
import org.teiid.query.sql.lang.TextTable;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.lang.WithQueryCommand;
import org.teiid.query.sql.lang.XMLTable;
import org.teiid.query.sql.navigator.PostOrderNavigator;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.MultipleElementSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;

public class SimpleQueryResolver implements CommandResolver {

    /** 
     * @see org.teiid.query.resolver.CommandResolver#resolveCommand(org.teiid.query.sql.lang.Command, org.teiid.query.metadata.TempMetadataAdapter, boolean)
     */
    public void resolveCommand(Command command, TempMetadataAdapter metadata, boolean resolveNullLiterals)
        throws QueryMetadataException, QueryResolverException, TeiidComponentException {

    	Query query = (Query) command;
    	
    	resolveWith(metadata, query);
        
        try {
            QueryResolverVisitor qrv = new QueryResolverVisitor(query, metadata);
            qrv.visit(query);
            ResolverVisitor visitor = (ResolverVisitor)qrv.getVisitor();
			visitor.throwException(true);
        } catch (TeiidRuntimeException e) {
            if (e.getCause() instanceof QueryMetadataException) {
                throw (QueryMetadataException)e.getCause();
            }
            if (e.getCause() instanceof QueryResolverException) {
                throw (QueryResolverException)e.getCause();
            }
            if (e.getCause() instanceof TeiidComponentException) {
                throw (TeiidComponentException)e.getCause();
            }
            throw e;
        }
                                       
        if (query.getLimit() != null) {
            ResolverUtil.resolveLimit(query.getLimit());
        }
        
        if (query.getOrderBy() != null) {
        	ResolverUtil.resolveOrderBy(query.getOrderBy(), query, metadata);
        }
        
        List<SingleElementSymbol> symbols = query.getSelect().getProjectedSymbols();
        
        if (query.getInto() != null) {
            GroupSymbol symbol = query.getInto().getGroup();
            ResolverUtil.resolveImplicitTempGroup(metadata, symbol, symbols);
        } else if (resolveNullLiterals) {
            ResolverUtil.resolveNullLiterals(symbols);
        }
    }

	static void resolveWith(TempMetadataAdapter metadata,
			QueryCommand query) throws QueryResolverException, TeiidComponentException {
		if (query.getWith() == null) {
			return;
		}
		LinkedHashSet<GroupSymbol> discoveredGroups = new LinkedHashSet<GroupSymbol>();
		for (WithQueryCommand obj : query.getWith()) {
            QueryCommand queryExpression = obj.getCommand();
            
            QueryResolver.setChildMetadata(queryExpression, query);
            
            QueryResolver.resolveCommand(queryExpression, metadata.getMetadata(), false);

            if (!discoveredGroups.add(obj.getGroupSymbol())) {
            	throw new QueryResolverException(QueryPlugin.Util.getString("SimpleQueryResolver.duplicate_with", obj.getGroupSymbol())); //$NON-NLS-1$
            }
            List<? extends SingleElementSymbol> projectedSymbols = obj.getCommand().getProjectedSymbols();
            if (obj.getColumns() != null && !obj.getColumns().isEmpty()) {
            	if (obj.getColumns().size() != projectedSymbols.size()) {
            		throw new QueryResolverException(QueryPlugin.Util.getString("SimpleQueryResolver.mismatched_with_columns", obj.getGroupSymbol())); //$NON-NLS-1$
            	}
            	Iterator<ElementSymbol> iter = obj.getColumns().iterator();
            	for (SingleElementSymbol singleElementSymbol : projectedSymbols) {
            		ElementSymbol es = iter.next();
            		es.setType(singleElementSymbol.getType());
				}
            	projectedSymbols = obj.getColumns();
            } 
            TempMetadataID id = ResolverUtil.addTempGroup(metadata, obj.getGroupSymbol(), projectedSymbols, true);
            obj.getGroupSymbol().setMetadataID(metadata.getMetadataStore().getTempGroupID(obj.getGroupSymbol().getName()));
            obj.getGroupSymbol().setIsTempTable(true);
            List<GroupSymbol> groups = Collections.singletonList(obj.getGroupSymbol());
            if (obj.getColumns() != null && !obj.getColumns().isEmpty()) {
	            for (SingleElementSymbol singleElementSymbol : projectedSymbols) {
	                ResolverVisitor.resolveLanguageObject(singleElementSymbol, groups, metadata);
				}
            }
            if (obj.getColumns() != null && !obj.getColumns().isEmpty()) {
            	Iterator<ElementSymbol> iter = obj.getColumns().iterator();
                for (TempMetadataID colid : id.getElements()) {
            		ElementSymbol es = iter.next();
            		es.setMetadataID(colid);
            		es.setGroupSymbol(obj.getGroupSymbol());
				}
            }
        }
	}

    private static GroupSymbol resolveAllInGroup(MultipleElementSymbol allInGroupSymbol, Set<GroupSymbol> groups, QueryMetadataInterface metadata) throws QueryResolverException, QueryMetadataException, TeiidComponentException {       
        String groupAlias = allInGroupSymbol.getGroup().getCanonicalName();
        List<GroupSymbol> groupSymbols = ResolverUtil.findMatchingGroups(groupAlias, groups, metadata);
        if(groupSymbols.isEmpty() || groupSymbols.size() > 1) {
            String msg = QueryPlugin.Util.getString(groupSymbols.isEmpty()?"ERR.015.008.0047":"SimpleQueryResolver.ambiguous_all_in_group", allInGroupSymbol);  //$NON-NLS-1$ //$NON-NLS-2$
            QueryResolverException qre = new QueryResolverException(msg);
            qre.addUnresolvedSymbol(new UnresolvedSymbolDescription(allInGroupSymbol.toString(), msg));
            throw qre;
        }
        GroupSymbol gs = allInGroupSymbol.getGroup();
        allInGroupSymbol.setGroup(groupSymbols.get(0).clone());
        allInGroupSymbol.getGroup().setOutputName(gs.getOutputName());
        return groupSymbols.get(0);
    }
    
    public static class QueryResolverVisitor extends PostOrderNavigator {

        private LinkedHashSet<GroupSymbol> currentGroups = new LinkedHashSet<GroupSymbol>();
        private LinkedList<GroupSymbol> discoveredGroups = new LinkedList<GroupSymbol>();
        private List<GroupSymbol> implicitGroups = new LinkedList<GroupSymbol>();
        private TempMetadataAdapter metadata;
        private Query query;
        private boolean allowImplicit = true;
        
        public QueryResolverVisitor(Query query, TempMetadataAdapter metadata) {
            super(new ResolverVisitor(metadata, null, query.getExternalGroupContexts()));
            ResolverVisitor visitor = (ResolverVisitor)getVisitor();
            visitor.setGroups(currentGroups);
            this.query = query;
            this.metadata = metadata;
        }
        
        protected void postVisitVisitor(LanguageObject obj) {
            super.postVisitVisitor(obj);
            ResolverVisitor visitor = (ResolverVisitor)getVisitor();
            try {
				visitor.throwException(false);
			} catch (TeiidException e) {
				throw new TeiidRuntimeException(e);
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
            } catch (TeiidException err) {
                throw new TeiidRuntimeException(err);
            }
        }
                        
        private void resolveSubQuery(SubqueryContainer<?> obj, Collection<GroupSymbol> externalGroups) {
            Command command = obj.getCommand();
            
            QueryResolver.setChildMetadata(command, query);
            command.pushNewResolvingContext(externalGroups);
            
            try {
                QueryResolver.resolveCommand(command, metadata.getMetadata(), false);
            } catch (TeiidException err) {
                throw new TeiidRuntimeException(err);
            }
        }
        
        public void visit(MultipleElementSymbol obj) {
        	// Determine group that this symbol is for
            try {
                List<ElementSymbol> elementSymbols = new ArrayList<ElementSymbol>();
                Collection<GroupSymbol> groups = currentGroups;
                if (obj.getGroup() != null) {
                	groups = Arrays.asList(resolveAllInGroup(obj, currentGroups, metadata));
                }
                for (GroupSymbol group : groups) {
                    elementSymbols.addAll(resolveSelectableElements(group));
                }
                obj.setElementSymbols(elementSymbols);
            } catch (TeiidException err) {
                throw new TeiidRuntimeException(err);
            } 
        }

        private List<ElementSymbol> resolveSelectableElements(GroupSymbol group) throws QueryMetadataException,
                                                                 TeiidComponentException {
            List<ElementSymbol> elements = ResolverUtil.resolveElementsInGroup(group, metadata);
            
            List<ElementSymbol> result = new ArrayList<ElementSymbol>(elements.size());
   
            // Look for elements that are not selectable and remove them
            for (ElementSymbol element : elements) {
                if(metadata.elementSupports(element.getMetadataID(), SupportConstants.Element.SELECT)) {
                    element = element.clone();
                    element.setGroupSymbol(group);
                	result.add(element);
                }
            }
            return result;
        }
        
        public void visit(ScalarSubquery obj) {
            resolveSubQuery(obj, this.currentGroups);
        }
        
        public void visit(ExistsCriteria obj) {
            resolveSubQuery(obj, this.currentGroups);
        }
        
        public void visit(SubqueryCompareCriteria obj) {
            visitNode(obj.getLeftExpression());
            resolveSubQuery(obj, this.currentGroups);
            postVisitVisitor(obj);
        }
        
        public void visit(SubquerySetCriteria obj) {
            visitNode(obj.getExpression());
            resolveSubQuery(obj, this.currentGroups);
            postVisitVisitor(obj);
        }
        
        @Override
        public void visit(TextTable obj) {
        	LinkedHashSet<GroupSymbol> saved = preTableFunctionReference(obj);
        	this.visitNode(obj.getFile());
        	try {
				obj.setFile(ResolverUtil.convertExpression(obj.getFile(), DataTypeManager.DefaultDataTypes.CLOB, metadata));
			} catch (QueryResolverException e) {
				throw new TeiidRuntimeException(e);
			}
			postTableFunctionReference(obj, saved);
            //set to fixed width if any column has width specified
            for (TextTable.TextColumn col : obj.getColumns()) {
				if (col.getWidth() != null) {
					obj.setFixedWidth(true);
					break;
				}
			}
        }
        
        @Override
        public void visit(ArrayTable obj) {
        	LinkedHashSet<GroupSymbol> saved = preTableFunctionReference(obj);
        	visitNode(obj.getArrayValue());
			postTableFunctionReference(obj, saved);
        }
        
        @Override
        public void visit(XMLTable obj) {
        	LinkedHashSet<GroupSymbol> saved = preTableFunctionReference(obj);
        	visitNodes(obj.getPassing());
			postTableFunctionReference(obj, saved);
			try {
	    		ResolverUtil.setDesiredType(obj.getPassing(), obj);
				obj.compileXqueryExpression();
				for (XMLTable.XMLColumn column : obj.getColumns()) {
					if (column.getDefaultExpression() == null) {
						continue;
					}
					visitNode(column.getDefaultExpression());
					Expression ex = ResolverUtil.convertExpression(column.getDefaultExpression(), DataTypeManager.getDataTypeName(column.getSymbol().getType()), metadata);
					column.setDefaultExpression(ex);
				}
			} catch (TeiidException e) {
				throw new TeiidRuntimeException(e);
			}
        }
        
        /**
		 * @param tfr  
		 */
        public LinkedHashSet<GroupSymbol> preTableFunctionReference(TableFunctionReference tfr) {
        	LinkedHashSet<GroupSymbol> saved = new LinkedHashSet<GroupSymbol>(this.currentGroups);
        	if (allowImplicit) {
        		currentGroups.addAll(this.implicitGroups);
        	}
        	return saved;
        }
        
        public void postTableFunctionReference(TableFunctionReference obj, LinkedHashSet<GroupSymbol> saved) {
			//we didn't create a true external context, so we manually mark external
			for (ElementSymbol symbol : ElementCollectorVisitor.getElements(obj, false)) {
				if (symbol.isExternalReference()) {
					continue;
				}
				if (implicitGroups.contains(symbol.getGroupSymbol())) {
					symbol.setIsExternalReference(true);
				}
			}
			if (allowImplicit) {
	        	this.currentGroups.clear();
	        	this.currentGroups.addAll(saved);
			}
            discoveredGroup(obj.getGroupSymbol());
            try {
                ResolverUtil.addTempGroup(metadata, obj.getGroupSymbol(), obj.getProjectedSymbols(), false);
            } catch (QueryResolverException err) {
                throw new TeiidRuntimeException(err);
            }
            obj.getGroupSymbol().setMetadataID(metadata.getMetadataStore().getTempGroupID(obj.getGroupSymbol().getName()));
            //now resolve the projected symbols
            Set<GroupSymbol> groups = new HashSet<GroupSymbol>();
            groups.add(obj.getGroupSymbol());
            for (ElementSymbol symbol : obj.getProjectedSymbols()) {
                try {
					ResolverVisitor.resolveLanguageObject(symbol, groups, null, metadata);
				} catch (TeiidException e) {
					throw new TeiidRuntimeException(e);
				}				
			}
        }
        
        public void visit(SubqueryFromClause obj) {
        	Collection<GroupSymbol> externalGroups = this.currentGroups;
        	if (obj.isTable() && allowImplicit) {
        		externalGroups = new ArrayList<GroupSymbol>(externalGroups);
        		externalGroups.addAll(this.implicitGroups);
        	}
            resolveSubQuery(obj, externalGroups);
            discoveredGroup(obj.getGroupSymbol());
            try {
                ResolverUtil.addTempGroup(metadata, obj.getGroupSymbol(), obj.getCommand().getProjectedSymbols(), false);
            } catch (QueryResolverException err) {
                throw new TeiidRuntimeException(err);
            }
            obj.getGroupSymbol().setMetadataID(metadata.getMetadataStore().getTempGroupID(obj.getGroupSymbol().getName())); 
        }
                        
        public void visit(UnaryFromClause obj) {
            GroupSymbol group = obj.getGroup();
            visitNode(group);
            try {
	            if (!group.isProcedure() && metadata.isXMLGroup(group.getMetadataID())) {
	                throw new QueryResolverException("ERR.015.008.0003", QueryPlugin.Util.getString("ERR.015.008.0003")); //$NON-NLS-1$ //$NON-NLS-2$
	            }
	            discoveredGroup(group);
	            if (group.isProcedure()) {
	                createProcRelational(obj);
	            }
            } catch(TeiidException e) {
                throw new TeiidRuntimeException(e);                        
			}
        }
        
        private void discoveredGroup(GroupSymbol group) {
        	discoveredGroups.add(group);
        	if (allowImplicit) {
        		implicitGroups.add(group);
        	}
        }

		private void createProcRelational(UnaryFromClause obj)
				throws TeiidComponentException, QueryMetadataException,
				QueryResolverException {
			GroupSymbol group = obj.getGroup();
			String fullName = metadata.getFullName(group.getMetadataID());
			String queryName = group.getName();
			
			StoredProcedureInfo storedProcedureInfo = metadata.getStoredProcedureInfoForProcedure(fullName);

			StoredProcedure storedProcedureCommand = new StoredProcedure();
			storedProcedureCommand.setProcedureRelational(true);
			storedProcedureCommand.setProcedureName(fullName);
			
			List<SPParameter> metadataParams = storedProcedureInfo.getParameters();
			
			Query procQuery = new Query();
			From from = new From();
			from.addClause(new SubqueryFromClause("X", storedProcedureCommand)); //$NON-NLS-1$
			procQuery.setFrom(from);
			Select select = new Select();
			select.addSymbol(new MultipleElementSymbol("X")); //$NON-NLS-1$
			procQuery.setSelect(select);
			
			List<String> accessPatternElementNames = new LinkedList<String>();
			
			int paramIndex = 1;
			
			for (SPParameter metadataParameter : metadataParams) {
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
			
			QueryResolver.resolveCommand(procQuery, metadata.getMetadata());
			
			List<SingleElementSymbol> projectedSymbols = procQuery.getProjectedSymbols();
			
			HashSet<String> foundNames = new HashSet<String>();
			
			for (SingleElementSymbol ses : projectedSymbols) {
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
         * @see org.teiid.query.sql.navigator.PreOrPostOrderNavigator#visit(org.teiid.query.sql.lang.Into)
         */
        public void visit(Into obj) {
            if (!obj.getGroup().isImplicitTempGroupSymbol()) {
                super.visit(obj);
            }
        }

        public void visit(JoinPredicate obj) {
            assert currentGroups.isEmpty();
        	List<GroupSymbol> tempImplicitGroups = new ArrayList<GroupSymbol>(discoveredGroups);
        	discoveredGroups.clear();
            visitNode(obj.getLeftClause());
            List<GroupSymbol> leftGroups = new ArrayList<GroupSymbol>(discoveredGroups);
        	discoveredGroups.clear();
            visitNode(obj.getRightClause());
            discoveredGroups.addAll(leftGroups);
            addDiscoveredGroups();
            visitNodes(obj.getJoinCriteria());
            discoveredGroups.addAll(currentGroups);
            currentGroups.clear();
            discoveredGroups.addAll(tempImplicitGroups);
        }

		private void addDiscoveredGroups() {
			for (GroupSymbol group : discoveredGroups) {
				if (!this.currentGroups.add(group)) {
	                String msg = QueryPlugin.Util.getString("ERR.015.008.0046", group.getName()); //$NON-NLS-1$
	                QueryResolverException qre = new QueryResolverException("ERR.015.008.0046", msg); //$NON-NLS-1$
	                qre.addUnresolvedSymbol(new UnresolvedSymbolDescription(group.toString(), msg));
	                throw new TeiidRuntimeException(qre);
	            }
			}
            discoveredGroups.clear();
		}
                
        public void visit(From obj) {
            assert currentGroups.isEmpty();
            for (FromClause clause : obj.getClauses()) {
				checkImplicit(clause);
			}
            super.visit(obj);
            addDiscoveredGroups();
        }

		private void checkImplicit(FromClause clause) {
			if (clause instanceof JoinPredicate) {
				JoinPredicate jp = (JoinPredicate)clause;
				if (jp.getJoinType() == JoinType.JOIN_FULL_OUTER || jp.getJoinType() == JoinType.JOIN_RIGHT_OUTER) {
					allowImplicit = false;
					return;
				}
				checkImplicit(jp.getLeftClause());
				if (allowImplicit) {
					checkImplicit(jp.getRightClause());
				}
			}
		}
    }
}
