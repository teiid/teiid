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

package org.teiid.query.optimizer.relational;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.DataPolicy.ResourceType;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.api.exception.query.QueryProcessingException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.Policy;
import org.teiid.metadata.Policy.Operation;
import org.teiid.query.QueryPlugin;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.ExpressionCriteria;
import org.teiid.query.sql.lang.FilteredCommand;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.lang.SetClause;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.visitor.CorrelatedReferenceCollectorVisitor;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.util.CommandContext;
import org.teiid.query.validator.ValidationVisitor;
import org.teiid.query.validator.Validator;
import org.teiid.query.validator.ValidatorFailure;
import org.teiid.query.validator.ValidatorReport;

public class RowBasedSecurityHelper {

    static class RecontextVisitor extends
            ExpressionMappingVisitor {
        private final GroupSymbol group;
        private final String definition;

        RecontextVisitor(GroupSymbol group) {
            super(null);
            this.group = group;
            this.definition = group.getDefinition();
        }

        @Override
        public Expression replaceExpression(
                Expression element) {
            if (element instanceof ElementSymbol) {
                ElementSymbol es = (ElementSymbol)element;
                if (es.getGroupSymbol().getDefinition() == null && es.getGroupSymbol().getName().equalsIgnoreCase(this.definition)) {
                    es.getGroupSymbol().setDefinition(group.getDefinition());
                    es.getGroupSymbol().setName(group.getName());
                    es.setOutputName(null);
                }
            }
            return element;
        }
    }

    public static boolean applyRowSecurity(QueryMetadataInterface metadata,
            final GroupSymbol group, CommandContext cc) throws QueryMetadataException, TeiidComponentException {
        Map<String, DataPolicy> policies = cc.getAllowedDataPolicies();
        if (policies == null || policies.isEmpty()) {
            return false;
        }
        String fullName = metadata.getFullName(group.getMetadataID());
        for (Map.Entry<String, DataPolicy> entry : policies.entrySet()) {
            DataPolicyMetadata dpm = (DataPolicyMetadata)entry.getValue();
            if (dpm.hasRowSecurity(fullName)) {
                return true;
            }
        }
        return false;
    }

    public static Criteria getRowBasedFilters(QueryMetadataInterface metadata,
            final GroupSymbol group, CommandContext cc, boolean constraintsOnly, Policy.Operation operation)
            throws QueryMetadataException, TeiidComponentException, TeiidProcessingException {
        Map<String, DataPolicy> policies = cc.getAllowedDataPolicies();
        if (policies == null || policies.isEmpty()) {
            return null;
        }
        boolean user = false;
        ArrayList<Criteria> crits = null;
        Object metadataID = group.getMetadataID();
        String fullName = metadata.getFullName(metadataID);
        for (Map.Entry<String, DataPolicy> entry : policies.entrySet()) {
            DataPolicyMetadata dpm = (DataPolicyMetadata)entry.getValue();
            PermissionMetaData pmd = dpm.getPermissionMetadata(fullName, group.isProcedure()?ResourceType.PROCEDURE:ResourceType.TABLE);
            Criteria filter = null;
            boolean added = false;
            if (pmd != null) {
                String filterString = pmd.getCondition();
                if (filterString != null && !(constraintsOnly && Boolean.FALSE.equals(pmd.getConstraint()))) {
                    filter = resolveCondition(metadata, group, fullName,
                            entry, pmd, filterString);
                    added = true;
                    if (crits == null) {
                        crits = new ArrayList<Criteria>(2);
                    }
                    crits.add(filter);
                }
            }
            Map<String, Policy> polices = dpm.getPolicies(group.isProcedure()?org.teiid.metadata.Database.ResourceType.PROCEDURE:org.teiid.metadata.Database.ResourceType.TABLE, fullName);
            if (polices != null) {
                for (Policy policy : polices.values()) {
                    if (!policy.appliesTo(operation)) {
                        continue;
                    }
                    if (crits == null) {
                        crits = new ArrayList<Criteria>(2);
                    }
                    crits.add(resolveCondition(metadata, group, fullName, entry, policy.getCondition()));
                    added = true;
                }
            }
            if (added && !dpm.isAnyAuthenticated()) {
                user = true;
            }
        }
        if (crits == null || crits.isEmpty()) {
            return null;
        }
        Criteria result = null;
        if (crits.size() == 1) {
            result = crits.get(0);
        } else {
            result = new CompoundCriteria(CompoundCriteria.OR, crits);
        }

        if (group.getDefinition() != null) {
            ExpressionMappingVisitor emv = new RecontextVisitor(group);
            PreOrPostOrderNavigator.doVisit(result, emv, PreOrPostOrderNavigator.PRE_ORDER, true);
        }
        //we treat this as user deterministic since the data roles won't change.  this may change if the logic becomes dynamic
        if (user) {
            cc.setDeterminismLevel(Determinism.USER_DETERMINISTIC);
        }
        Expression ex = QueryRewriter.rewriteExpression(result, cc, metadata, true);
        if (ex instanceof Criteria) {
            return (Criteria)ex;
        }
        return QueryRewriter.rewriteCriteria(new ExpressionCriteria(ex), cc, metadata);
    }

    static Criteria resolveCondition(QueryMetadataInterface metadata,
            final GroupSymbol group, String fullName, Map.Entry<String, DataPolicy> entry,
            PermissionMetaData pmd, String filterString) throws QueryMetadataException {
        Criteria filter = (Criteria)pmd.getResolvedCondition();
        if (filter == null) {
            filter = resolveCondition(metadata, group, fullName, entry,
                    filterString);
            pmd.setResolvedCondition(filter.clone());
        } else {
            filter = (Criteria) filter.clone();
        }
        return filter;
    }

    static Criteria resolveCondition(Policy policy, QueryMetadataInterface metadata,
            final GroupSymbol group, String fullName, Map.Entry<String, DataPolicy> entry, String filterString) throws TeiidComponentException {
        Criteria filter = (Criteria)metadata.getFromMetadataCache(policy, ""); //$NON-NLS-1$
        if (filter == null) {
            filter = resolveCondition(metadata, group, fullName, entry,
                    filterString);
            metadata.addToMetadataCache(policy, "", filter.clone()); //$NON-NLS-1$
        } else {
            filter = (Criteria) filter.clone();
        }
        return filter;
    }

    private static Criteria resolveCondition(QueryMetadataInterface metadata,
            final GroupSymbol group, String fullName,
            Map.Entry<String, DataPolicy> entry, String filterString) throws QueryMetadataException {
        try {
            Criteria filter = QueryParser.getQueryParser().parseCriteria(filterString);
            GroupSymbol gs = group;
            if (group.getDefinition() != null) {
                gs = new GroupSymbol(fullName);
                gs.setMetadataID(group.getMetadataID());
            }
            Collection<GroupSymbol> groups = Arrays.asList(gs);
            for (SubqueryContainer container : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(filter)) {
                container.getCommand().pushNewResolvingContext(groups);
                QueryResolver.resolveCommand(container.getCommand(), metadata, false);
            }
            ResolverVisitor.resolveLanguageObject(filter, groups, metadata);
            ValidatorReport report = Validator.validate(filter, metadata, new ValidationVisitor());
            if (report.hasItems()) {
                ValidatorFailure firstFailure = report.getItems().iterator().next();
                throw new QueryMetadataException(QueryPlugin.Event.TEIID31129, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31129, entry.getKey(), fullName) + " " + firstFailure); //$NON-NLS-1$
            }
            return filter;
        } catch (QueryMetadataException e) {
            throw e;
        } catch (TeiidException e) {
            throw new QueryMetadataException(QueryPlugin.Event.TEIID31129, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31129, entry.getKey(), fullName));
        }
    }

    public static Command checkUpdateRowBasedFilters(ProcedureContainer container, Command procedure, RelationalPlanner planner)
            throws QueryMetadataException, TeiidComponentException,
            TeiidProcessingException, QueryResolverException {
        if (container instanceof StoredProcedure) {
            return procedure;
        }
        Policy.Operation operation = Operation.UPDATE;
        if (container.getType() == Command.TYPE_DELETE) {
            operation = Operation.DELETE;
        } else if (container.getType() == Command.TYPE_INSERT) {
            operation = Operation.INSERT;
        }
        Criteria filter = RowBasedSecurityHelper.getRowBasedFilters(planner.metadata, container.getGroup(), planner.context, false, operation);
        if (filter == null) {
            return procedure;
        }
        addFilter(container, planner, filter);
        //we won't enforce on the update side through a virtual
        if (procedure != null) {
            return procedure;
        }
        filter = RowBasedSecurityHelper.getRowBasedFilters(planner.metadata, container.getGroup(), planner.context, true, operation);
        if (filter == null) {
            return procedure;
        }
        //TODO: alter the compensation logic in RelationalPlanner to produce an row-by-row check for insert/update
        //check constraints
        Map<ElementSymbol, Expression> values = null;
        boolean compensate = false;
        if (container instanceof Update) {
            Collection<ElementSymbol> elems = ElementCollectorVisitor.getElements(filter, true);
            //check the change set against the filter
            values = new HashMap<ElementSymbol, Expression>();
            Update update = (Update)container;
            boolean constraintApplicable = false;
            for (SetClause clause : update.getChangeList().getClauses()) {
                if (!elems.contains(clause.getSymbol())) {
                    continue;
                }
                constraintApplicable = true;
                //TODO: do this is a single eval pass
                if (EvaluatableVisitor.isFullyEvaluatable(clause.getValue(), true)) {
                    values.put(clause.getSymbol(), clause.getValue());
                } else if (!compensate && !EvaluatableVisitor.isFullyEvaluatable(clause.getValue(), false)) {
                    compensate = true;
                }
            }
            if (!constraintApplicable) {
                return procedure;
            }
            //TOOD: decompose the where clause to see if there are any more static
            //values that can be plugged in
        } else if (container instanceof Insert) {
            Insert insert = (Insert)container;

            if (insert.getQueryExpression() == null) {
                values = new HashMap<ElementSymbol, Expression>();

                Collection<ElementSymbol> insertElmnts = ResolverUtil.resolveElementsInGroup(insert.getGroup(), planner.metadata);
                Collection<ElementSymbol> elems = null;

                for (ElementSymbol elementSymbol : insertElmnts) {
                    Expression value = null;
                    int index = insert.getVariables().indexOf(elementSymbol);
                    if (index == -1) {
                        try {
                            value = ResolverUtil.getDefault(elementSymbol, planner.metadata);
                            values.put(elementSymbol, value);
                        } catch (QueryResolverException e) {
                            if (elems == null) {
                                elems = ElementCollectorVisitor.getElements(filter, true);
                            }
                            if (elems.contains(elementSymbol)) {
                                throw new QueryProcessingException(QueryPlugin.Event.TEIID31295, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31295, elementSymbol));
                            }
                        }
                    } else {
                        value = (Expression) insert.getValues().get(index);
                        if (EvaluatableVisitor.isFullyEvaluatable(value, true)) {
                            values.put(elementSymbol, value);
                        }
                    }
                }
            } else {
                validateAndPlanSubqueries(filter, container.getGroup(), planner);
                insert.setConstraint(filter);
            }
        }
        if (values != null) {
            if (!values.isEmpty()) {
                ExpressionMappingVisitor.mapExpressions(filter, values);
                filter = QueryRewriter.rewriteCriteria(filter, planner.context, planner.metadata);
            }
            if (filter != QueryRewriter.TRUE_CRITERIA) {
                if (filter == QueryRewriter.FALSE_CRITERIA || filter == QueryRewriter.UNKNOWN_CRITERIA) {
                    throw new TeiidProcessingException(QueryPlugin.Event.TEIID31130, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31130, container));
                }
                if (container instanceof Update) {
                    Collection<ElementSymbol> elems = ElementCollectorVisitor.getElements(filter, true);
                    Update update = (Update)container;
                    if (!Collections.disjoint(elems, update.getChangeList().getClauseMap().keySet())) {
                        validateAndPlanSubqueries(filter, container.getGroup(), planner);
                        update.setConstraint(filter);
                        if (compensate) {
                            try {
                                planner.validateRowProcessing(container);
                            } catch (QueryPlannerException e) {
                                throw new TeiidProcessingException(QueryPlugin.Event.TEIID31131, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31131, container));
                            }
                            return QueryRewriter.createUpdateProcedure((Update)container, planner.metadata, planner.context);
                        }
                    }
                } else if (container instanceof Insert) {
                    validateAndPlanSubqueries(filter, container.getGroup(), planner);
                    ((Insert)container).setConstraint(filter);
                }
            }
        }
        return procedure;
    }

    /**
     * because of the way constraints are enforced, we cannot allow correlated references
     * the issues are:
     *   - the insert may be partially specified and relevant default values may not be known
     *   - to know the full update row, we have to use row processing
     * @param object
     * @param gs
     * @param planner
     * @throws QueryValidatorException
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     * @throws QueryPlannerException
     */
    private static void validateAndPlanSubqueries(LanguageObject object, GroupSymbol gs, RelationalPlanner planner) throws QueryValidatorException, QueryPlannerException, QueryMetadataException, TeiidComponentException {
        List<SubqueryContainer<?>> subqueries = ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(object);
        if (subqueries.isEmpty()) {
            return;
        }
        Set<GroupSymbol> groups = Collections.singleton(gs);
        planner.planSubqueries(groups, null, subqueries, true, false);
        List<Reference> refs = new LinkedList<Reference>();
        CorrelatedReferenceCollectorVisitor.collectReferences(object, groups, refs, planner.metadata);
        if (!refs.isEmpty()) {
            throw new QueryValidatorException(QueryPlugin.Event.TEIID31142, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31142, object, gs));
        }
    }

    private static void addFilter(ProcedureContainer container,
            RelationalPlanner planner, Criteria filter) {
        if (container instanceof FilteredCommand) {
            FilteredCommand fc = (FilteredCommand)container;
            if (fc.getCriteria() == null) {
                fc.setCriteria(filter);
            } else {
                fc.setCriteria(QueryRewriter.optimizeCriteria(new CompoundCriteria(Arrays.asList(fc.getCriteria(), filter)), planner.metadata));
            }
        }
    }

    public static void checkConstraints(Command atomicCommand, Evaluator eval)
            throws ExpressionEvaluationException, BlockedException,
            TeiidComponentException, QueryProcessingException {
        Criteria constraint = null;
        HashMap<ElementSymbol, Expression> values = null;
        int rows = -1;
        if (atomicCommand instanceof Update) {
            Update update = (Update)atomicCommand;
            constraint = update.getConstraint();
            if (constraint != null) {
                values = new HashMap<ElementSymbol, Expression>();
                for (SetClause clause : update.getChangeList().getClauses()) {
                    values.put(clause.getSymbol(), clause.getValue());
                    if (rows == -1) {
                        rows = getMultiValuedSize(clause.getValue());
                    }
                }
            }
        } else if (atomicCommand instanceof Insert) {
            Insert insert = (Insert)atomicCommand;
            constraint = insert.getConstraint();
            if (constraint != null) {
                values = new HashMap<ElementSymbol, Expression>();

                if (insert.getQueryExpression() == null) {
                    for (int i = 0; i < insert.getVariables().size(); i++) {
                        ElementSymbol symbol = insert.getVariables().get(i);
                        Expression value = (Expression) insert.getValues().get(i);
                        values.put(symbol, value);
                        if (rows == -1) {
                            rows = getMultiValuedSize(value);
                        }
                    }
                }
            }
        } else if (atomicCommand instanceof BatchedUpdateCommand) {
            BatchedUpdateCommand buc = (BatchedUpdateCommand)atomicCommand;
            List<Command> commands = buc.getUpdateCommands();
            for (Command command : commands) {
                checkConstraints(command, eval);
            }
            return;
        }
        if (constraint == null) {
            return;
        }
        if (rows != -1) {
            Map<ElementSymbol, Expression> currentValues = new HashMap<ElementSymbol, Expression>();
            for (int i = 0; i < rows; i++) {
                currentValues.clear();
                for (Map.Entry<ElementSymbol, Expression> entry : values.entrySet()) {
                    ElementSymbol symbol = entry.getKey();
                    Expression value = entry.getValue();
                    if (value instanceof Constant && ((Constant)value).isMultiValued()) {
                        Object obj =  ((List<?>)((Constant)value).getValue()).get(i);
                        value = new Constant(obj, symbol.getType());
                    }
                    currentValues.put(symbol, value);
                }
                evaluateConstraint(atomicCommand, eval, constraint, currentValues);
            }
        } else {
            evaluateConstraint(atomicCommand, eval, constraint, values);
        }
    }

    private static void evaluateConstraint(Command atomicCommand,
            Evaluator eval, Criteria constraint,
            Map<ElementSymbol, Expression> values)
            throws BlockedException,
            TeiidComponentException, QueryProcessingException {
        Criteria clone = (Criteria) constraint.clone();
        ExpressionMappingVisitor.mapExpressions(clone, values);
        try {
            if (!eval.evaluate(clone, null)) {
                throw new QueryProcessingException(QueryPlugin.Event.TEIID31130, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31130, atomicCommand));
            }
        } catch (ExpressionEvaluationException e) {
            throw new QueryProcessingException(QueryPlugin.Event.TEIID31130, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31130, atomicCommand));
        }
    }

    private static int getMultiValuedSize(Expression value) {
        if (value instanceof Constant && ((Constant)value).isMultiValued()) {
            return ((List<?>)((Constant)value).getValue()).size();
        }
        return 1;
    }

}
