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

package org.teiid.query.resolver.command;

import java.util.*;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.UnresolvedSymbolDescription;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionLibrary;
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
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.navigator.PostOrderNavigator;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.symbol.*;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.translator.SourceSystemFunctions;

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
            if (visitor.hasUserDefinedAggregate()) {
                ExpressionMappingVisitor emv = new ExpressionMappingVisitor(null) {
                    public Expression replaceExpression(Expression element) {
                        if (element instanceof Function && !(element instanceof AggregateSymbol) && ((Function) element).isAggregate()) {
                            Function f = (Function)element;
                            AggregateSymbol as = new AggregateSymbol(f.getName(), false, f.getArgs(), null);
                            as.setType(f.getType());
                            as.setFunctionDescriptor(f.getFunctionDescriptor());
                            return as;
                        }
                        return element;
                    }
                };
                PreOrPostOrderNavigator.doVisit(query, emv, PreOrPostOrderNavigator.POST_ORDER);
            }
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

        List<Expression> symbols = query.getSelect().getProjectedSymbols();

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

            QueryCommand recursive = null;

            try {
                QueryResolver.resolveCommand(queryExpression, metadata.getMetadata(), false);
            } catch (QueryResolverException e) {
                if (!(queryExpression instanceof SetQuery)) {
                    throw e;
                }
                SetQuery setQuery = (SetQuery)queryExpression;
                //valid form must be a union with nothing above
                if (setQuery.getOperation() != Operation.UNION
                        || setQuery.getLimit() != null
                        || setQuery.getOrderBy() != null
                        || setQuery.getOption() != null) {
                    throw e;
                }
                QueryResolver.resolveCommand(setQuery.getLeftQuery(), metadata.getMetadata(), false);
                recursive = setQuery.getRightQuery();
            }

            if (!discoveredGroups.add(obj.getGroupSymbol())) {
                 throw new QueryResolverException(QueryPlugin.Event.TEIID30101, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30101, obj.getGroupSymbol()));
            }
            List<? extends Expression> projectedSymbols = obj.getCommand().getProjectedSymbols();
            if (obj.getColumns() != null && !obj.getColumns().isEmpty()) {
                if (obj.getColumns().size() != projectedSymbols.size()) {
                     throw new QueryResolverException(QueryPlugin.Event.TEIID30102, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30102, obj.getGroupSymbol()));
                }
                Iterator<ElementSymbol> iter = obj.getColumns().iterator();
                for (Expression singleElementSymbol : projectedSymbols) {
                    ElementSymbol es = iter.next();
                    es.setType(singleElementSymbol.getType());
                }
                projectedSymbols = obj.getColumns();
            }
            TempMetadataID id = (TempMetadataID) obj.getGroupSymbol().getMetadataID();
            TempMetadataID newId = ResolverUtil.addTempGroup(metadata, obj.getGroupSymbol(), projectedSymbols, true);
            if (id == null) {
                id = newId;
            } else {
                //we track with clause groups by identity, so create a new id only when it doesn't exist
                //we purposely in rewrite keep the definition of the with group consistent
                if (!newId.getElements().equals(id.getElements())) {
                    throw new TeiidRuntimeException("Planning error with common table " + newId); //$NON-NLS-1$
                }
                metadata.getMetadataStore().getData().put(id.getID(), id);
            }
            obj.getGroupSymbol().setMetadataID(metadata.getMetadataStore().getTempGroupID(obj.getGroupSymbol().getName()));
            obj.getGroupSymbol().setIsTempTable(true);
            List<GroupSymbol> groups = Collections.singletonList(obj.getGroupSymbol());
            if (obj.getColumns() != null && !obj.getColumns().isEmpty()) {
                for (Expression singleElementSymbol : projectedSymbols) {
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

            if (recursive != null) {
                QueryResolver.setChildMetadata(recursive, query);
                QueryResolver.resolveCommand(recursive, metadata.getMetadata(), false);
                new SetQueryResolver().resolveSetQuery(metadata, false, (SetQuery)queryExpression, ((SetQuery)queryExpression).getLeftQuery(), recursive);
                obj.setRecursive(true);
            }
        }
    }

    private static GroupSymbol resolveAllInGroup(MultipleElementSymbol allInGroupSymbol, Set<GroupSymbol> groups, QueryMetadataInterface metadata) throws QueryResolverException, QueryMetadataException, TeiidComponentException {
        String groupAlias = allInGroupSymbol.getGroup().getName();
        List<GroupSymbol> groupSymbols = ResolverUtil.findMatchingGroups(groupAlias, groups, metadata);
        if(groupSymbols.isEmpty() || groupSymbols.size() > 1) {
            String msg = QueryPlugin.Util.getString(groupSymbols.isEmpty()?"ERR.015.008.0047":"SimpleQueryResolver.ambiguous_all_in_group", allInGroupSymbol);  //$NON-NLS-1$ //$NON-NLS-2$
            QueryResolverException qre = new QueryResolverException(msg);
            qre.addUnresolvedSymbol(new UnresolvedSymbolDescription(allInGroupSymbol.toString(), msg));
            throw qre;
        }
        allInGroupSymbol.setGroup(groupSymbols.get(0).clone());
        return groupSymbols.get(0);
    }

    public static class QueryResolverVisitor extends PostOrderNavigator {

        private LinkedHashSet<GroupSymbol> currentGroups = new LinkedHashSet<GroupSymbol>();
        private LinkedList<GroupSymbol> discoveredGroups = new LinkedList<GroupSymbol>();
        private List<GroupSymbol> implicitGroups = new LinkedList<GroupSymbol>();
        private TempMetadataAdapter metadata;
        private Query query;

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
                 throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30103, e);
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
            GroupBy groupBy = obj.getGroupBy();
            if (groupBy != null) {
                Object var = DQPWorkContext.getWorkContext().getSession().getSessionVariables().get("resolve_groupby_positional"); //$NON-NLS-1$
                if (Boolean.TRUE.equals(var)) {
                    for (int i = 0; i < groupBy.getCount(); i++) {
                        List<Expression> select = obj.getSelect().getProjectedSymbols();
                        Expression ex = groupBy.getSymbols().get(i);
                        ex = SymbolMap.getExpression(ex);
                        if (ex instanceof Constant && ex.getType() == DataTypeManager.DefaultDataClasses.INTEGER) {
                            Integer val = (Integer) ((Constant)ex).getValue();
                            if (val != null && val > 0 && val <= select.size()) {
                                Expression selectExpression = select.get(val - 1);
                                selectExpression = SymbolMap.getExpression(selectExpression);
                                groupBy.getSymbols().set(i, (Expression) selectExpression.clone());
                            }
                        }
                    }
                }
            }
            visitNode(obj.getLimit());
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
            for (GroupSymbol gs : externalGroups) {
                //subquery from clauses are not valid for resolving against
                //and they are not caught by later validation like scalar groups
                //we can directly remove as each command has a copy of the known temp groups
                if (!gs.isTempTable()) {
                    command.getTemporaryMetadata().removeTempGroup(gs.getName());
                }
            }

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
                if(metadata.elementSupports(element.getMetadataID(), SupportConstants.Element.SELECT) && !metadata.isPseudo(element.getMetadataID())) {
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
            if (obj.getCommand() != null) {
                resolveSubQuery(obj, this.currentGroups);
            }
            visitNode(obj.getArrayExpression());
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

        @Override
        public void visit(JsonTable obj) {
            LinkedHashSet<GroupSymbol> saved = preTableFunctionReference(obj);
            visitNode(obj.getJson());
            postTableFunctionReference(obj, saved);
            try {
                ResolverUtil.setDesiredType(obj.getJson(), DataTypeManager.DefaultDataClasses.CLOB, obj);
                //obj.compileXqueryExpression();
            } catch (TeiidException e) {
                throw new TeiidRuntimeException(e);
            }
            FunctionLibrary funcLibrary = this.metadata.getFunctionLibrary();
            if (!funcLibrary.getSystemFunctions().hasFunctionWithName(SourceSystemFunctions.JSONTOARRAY)) {
                throw new TeiidRuntimeException(new QueryResolverException(QueryPlugin.Event.TEIID31298, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31298)));
            }
        }

        @Override
        public void visit(ObjectTable obj) {
            LinkedHashSet<GroupSymbol> saved = preTableFunctionReference(obj);
            visitNodes(obj.getPassing());
            postTableFunctionReference(obj, saved);
            try {
                ResolverUtil.setDesiredType(obj.getPassing(), obj, DataTypeManager.DefaultDataClasses.OBJECT);
                for (ObjectTable.ObjectColumn column : obj.getColumns()) {
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
            currentGroups.addAll(this.implicitGroups);
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
            this.currentGroups.clear();
            this.currentGroups.addAll(saved);
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
            if (obj.isLateral()) {
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
            implicitGroups.add(group);
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

                    Expression newSymbol = new AliasSymbol(aliasName, new ExpressionSymbol(paramSymbol.getShortName(), ref));

                    select.addSymbol(newSymbol);
                    accessPatternElementNames.add(queryName + Symbol.SEPARATOR + aliasName);
                }
            }

            QueryResolver.resolveCommand(procQuery, metadata.getMetadata());

            List<Expression> projectedSymbols = procQuery.getProjectedSymbols();

            Set<String> foundNames = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);

            for (Expression ses : projectedSymbols) {
                if (!foundNames.add(Symbol.getShortName(ses))) {
                     throw new QueryResolverException(QueryPlugin.Event.TEIID30114, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30114, fullName));
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
            boolean allowReferences = true;
            if (obj.getJoinType() == JoinType.JOIN_RIGHT_OUTER || obj.getJoinType() == JoinType.JOIN_FULL_OUTER) {
                this.implicitGroups.removeAll(leftGroups);
                allowReferences = false;
            }
            try {
                visitNode(obj.getRightClause());
            } catch (TeiidRuntimeException e) {
                if (!allowReferences &&
                        (obj.getRightClause() instanceof TableFunctionReference || (obj.getRightClause() instanceof SubqueryFromClause && ((SubqueryFromClause)obj.getRightClause()).isLateral()))) {
                    //detect if this element symbol can't be used as part of a lateral join
                    QueryResolverException qre = (QueryResolverException)e.getCause();
                    for (UnresolvedSymbolDescription usd : qre.getUnresolvedSymbols()) {
                        if (usd.getObject() instanceof ElementSymbol) {
                            try {
                                ResolverVisitor.resolveLanguageObject(((ElementSymbol)usd.getObject()).getGroupSymbol(), leftGroups, metadata);
                                throw new TeiidRuntimeException(
                                        new QueryResolverException(QueryPlugin.Event.TEIID31268, qre, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31268, usd.getObject())));
                            } catch (QueryResolverException e1) {
                            } catch (TeiidComponentException e1) {
                            }
                        }
                    }
                }
                throw e;
            }
            if (!allowReferences) {
                this.implicitGroups.addAll(leftGroups);
            }
            //add to the beginning to maintain stable order
            discoveredGroups.addAll(0, leftGroups);
            addDiscoveredGroups();
            visitNodes(obj.getJoinCriteria());
            if (!discoveredGroups.isEmpty()) {
                throw new AssertionError();
            }
            discoveredGroups.addAll(currentGroups);
            currentGroups.clear();
            //add to the beginning to maintain stable order
            discoveredGroups.addAll(0, tempImplicitGroups);
        }

        private void addDiscoveredGroups() {
            for (GroupSymbol group : discoveredGroups) {
                if (!this.currentGroups.add(group)) {
                    String msg = QueryPlugin.Util.getString("ERR.015.008.0046", group.getName()); //$NON-NLS-1$
                    QueryResolverException qre = new QueryResolverException(QueryPlugin.Event.TEIID30115, msg);
                    qre.addUnresolvedSymbol(new UnresolvedSymbolDescription(group.toString(), msg));
                     throw new TeiidRuntimeException(qre);
                }
            }
            discoveredGroups.clear();
        }

        public void visit(From obj) {
            assert currentGroups.isEmpty();
            super.visit(obj);
            addDiscoveredGroups();
        }

        @Override
        public void visit(Limit obj) {
            super.visit(obj);
            if (obj.getOffset() != null) {
                ResolverUtil.setTypeIfNull(obj.getOffset(), DataTypeManager.DefaultDataClasses.INTEGER);
                try {
                    obj.setOffset(ResolverUtil.convertExpression(obj.getOffset(), DataTypeManager.DefaultDataTypes.INTEGER, metadata));
                } catch (QueryResolverException e) {
                    throw new TeiidRuntimeException(e);
                }
            }
            if (obj.getRowLimit() != null) {
                ResolverUtil.setTypeIfNull(obj.getRowLimit(), DataTypeManager.DefaultDataClasses.INTEGER);
                try {
                    obj.setRowLimit(ResolverUtil.convertExpression(obj.getRowLimit(), DataTypeManager.DefaultDataTypes.INTEGER, metadata));
                } catch (QueryResolverException e) {
                    throw new TeiidRuntimeException(e);
                }
            }
        }
    }
}
