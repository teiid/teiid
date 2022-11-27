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
import java.util.List;
import java.util.Map;

import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.DataPolicy.ResourceType;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.SearchedCaseExpression;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.util.CommandContext;
import org.teiid.query.validator.ValidationVisitor;
import org.teiid.query.validator.Validator;
import org.teiid.query.validator.ValidatorFailure;
import org.teiid.query.validator.ValidatorReport;

public class ColumnMaskingHelper {

    private static class WhenThen implements Comparable<WhenThen> {
        int order;
        Criteria when;
        Expression then;

        public WhenThen(Integer order, Criteria when, Expression then) {
            this.order = (order == null?0:order);
            this.when = when;
            this.then = then;
        }

        @Override
        public int compareTo(WhenThen arg0) {
            return arg0.order - order; //highest first
        }
    }

    /**
     * Create a masked version of the col.  Will return either the col if no masking or an {@link ExpressionSymbol} with the masked expression
     */
    private static Expression maskColumn(ElementSymbol col, GroupSymbol unaliased, QueryMetadataInterface metadata, ExpressionMappingVisitor emv, Map<String, DataPolicy> policies, CommandContext cc) throws TeiidComponentException, TeiidProcessingException {
        Object metadataID = col.getMetadataID();
        String fullName = metadata.getFullName(metadataID);
        final GroupSymbol group = col.getGroupSymbol();
        String elementType = metadata.getElementRuntimeTypeName(col.getMetadataID());
        Class<?> expectedType = DataTypeManager.getDataTypeClass(elementType);
        List<WhenThen> cases = null;
        Collection<GroupSymbol> groups = Arrays.asList(unaliased);
        for (Map.Entry<String, DataPolicy> entry : policies.entrySet()) {
            DataPolicyMetadata dpm = (DataPolicyMetadata)entry.getValue();
            PermissionMetaData pmd = dpm.getPermissionMetadata(fullName, ResourceType.COLUMN);
            if (pmd == null) {
                continue;
            }
            String maskString = pmd.getMask();
            if (maskString == null) {
                continue;
            }
            Criteria condition = null;
            if (pmd.getCondition() != null) {
                condition = RowBasedSecurityHelper.resolveCondition(metadata, group, metadata.getFullName(group.getMetadataID()), entry, pmd, pmd.getCondition());
            } else {
                condition = QueryRewriter.TRUE_CRITERIA;
            }
            Expression mask = (Expression)pmd.getResolvedMask();
            if (mask == null) {
                try {
                    mask = QueryParser.getQueryParser().parseExpression(pmd.getMask());
                    for (SubqueryContainer container : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(mask)) {
                        container.getCommand().pushNewResolvingContext(groups);
                        QueryResolver.resolveCommand(container.getCommand(), metadata, false);
                    }
                    ResolverVisitor.resolveLanguageObject(mask, groups, metadata);
                    ValidatorReport report = Validator.validate(mask, metadata, new ValidationVisitor());
                    if (report.hasItems()) {
                        ValidatorFailure firstFailure = report.getItems().iterator().next();
                        throw new QueryMetadataException(QueryPlugin.Event.TEIID31139, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31139, dpm.getName(), fullName) + " " + firstFailure); //$NON-NLS-1$
                    }
                    if (mask.getType() != expectedType) {
                        mask = ResolverUtil.convertExpression(mask, elementType, metadata);
                    }
                    pmd.setResolvedMask(mask.clone());
                    if (!dpm.isAnyAuthenticated()) {
                        //we treat this as user deterministic since the data roles won't change.  this may change if the logic becomes dynamic
                        //TODO: this condition may not even be used
                        cc.setDeterminismLevel(Determinism.USER_DETERMINISTIC);
                    }
                } catch (QueryMetadataException e) {
                    throw e;
                } catch (TeiidException e) {
                    throw new QueryMetadataException(QueryPlugin.Event.TEIID31129, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31129, dpm.getName(), fullName));
                }
            } else {
                mask = (Expression) mask.clone();
            }
            if (group.getDefinition() != null) {
                PreOrPostOrderNavigator.doVisit(mask, emv, PreOrPostOrderNavigator.PRE_ORDER, true);
                PreOrPostOrderNavigator.doVisit(condition, emv, PreOrPostOrderNavigator.PRE_ORDER, true);
            }
            if (cases == null) {
                cases = new ArrayList<ColumnMaskingHelper.WhenThen>();
            }
            cases.add(new WhenThen(pmd.getOrder(), condition, mask));
        }
        if (cases == null) {
            return col;
        }
        Collections.sort(cases);
        List<Criteria> whens = new ArrayList<Criteria>();
        List<Expression> thens = new ArrayList<Expression>();
        for (WhenThen whenThen : cases) {
            whens.add(whenThen.when);
            thens.add(whenThen.then);
        }
        SearchedCaseExpression sce = new SearchedCaseExpression(whens, thens);
        sce.setElseExpression(col);
        sce.setType(expectedType);
        Expression mask = QueryRewriter.rewriteExpression(sce, cc, metadata, true);
        return new ExpressionSymbol(col.getShortName(), mask);
    }

    public static List<? extends Expression> maskColumns(List<ElementSymbol> cols,
            final GroupSymbol group, QueryMetadataInterface metadata,
            CommandContext cc) throws QueryMetadataException, TeiidComponentException, TeiidProcessingException {
        Map<String, DataPolicy> policies = cc.getAllowedDataPolicies();
        if (policies == null || policies.isEmpty()) {
            return cols;
        }

        ArrayList<Expression> result = new ArrayList<Expression>(cols.size());
        ExpressionMappingVisitor emv = new RowBasedSecurityHelper.RecontextVisitor(group);
        GroupSymbol gs = group;
        if (gs.getDefinition() != null) {
            gs = new GroupSymbol(metadata.getFullName(group.getMetadataID()));
            gs.setMetadataID(group.getMetadataID());
        }
        for (int i = 0; i < cols.size(); i++) {
            result.add(maskColumn(cols.get(i), gs, metadata, emv, policies, cc));
        }
        return result;
    }

}
