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

package org.teiid.query.optimizer.relational.rules;

import java.util.Arrays;
import java.util.Collection;
import java.util.TreeSet;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.client.plan.Annotation;
import org.teiid.client.plan.Annotation.Priority;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.StringUtil;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SupportConstants;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.relational.AccessNode;
import org.teiid.query.processor.relational.LimitNode;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.AbstractCompareCriteria;
import org.teiid.query.sql.lang.AbstractSetCriteria;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.IsDistinctCriteria;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.MatchCriteria;
import org.teiid.query.sql.lang.NotCriteria;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.Array;
import org.teiid.query.sql.symbol.CaseExpression;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.JSONObject;
import org.teiid.query.sql.symbol.QueryString;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.symbol.SearchedCaseExpression;
import org.teiid.query.sql.symbol.TextLine;
import org.teiid.query.sql.symbol.WindowFunction;
import org.teiid.query.sql.symbol.XMLAttributes;
import org.teiid.query.sql.symbol.XMLCast;
import org.teiid.query.sql.symbol.XMLElement;
import org.teiid.query.sql.symbol.XMLExists;
import org.teiid.query.sql.symbol.XMLForest;
import org.teiid.query.sql.symbol.XMLNamespaces;
import org.teiid.query.sql.symbol.XMLParse;
import org.teiid.query.sql.symbol.XMLQuery;
import org.teiid.query.sql.symbol.XMLSerialize;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.sql.visitor.EvaluatableVisitor.EvaluationLevel;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.ExecutionFactory.Format;
import org.teiid.translator.SourceSystemFunctions;


/**
 */
public class CriteriaCapabilityValidatorVisitor extends LanguageVisitor {

    public static class ValidatorOptions {
        boolean isJoin;
        boolean isSelectClause;
        boolean multiValuedReferences;
        boolean pushdown;

        public ValidatorOptions() {

        }

        public ValidatorOptions(boolean isJoin, boolean isSelectClause,
                boolean multiValuedReferences) {
            this.isJoin = isJoin;
            this.isSelectClause = isSelectClause;
            this.multiValuedReferences = multiValuedReferences;
        }

        ValidatorOptions pushdown(boolean b) {
            this.pushdown = b;
            return this;
        }
    }

    // Initialization state
    private Object modelID;
    private QueryMetadataInterface metadata;
    private CapabilitiesFinder capFinder;
    private AnalysisRecord analysisRecord;

    // Retrieved during initialization and cached
    private SourceCapabilities caps;

    // Output state
    private TeiidComponentException exception;
    private boolean valid = true;

    // contextual state
    private boolean isJoin;
    private boolean isSelectClause;
    private boolean checkEvaluation = true;
    private boolean pushdown;

    public CriteriaCapabilityValidatorVisitor(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, SourceCapabilities caps) throws QueryMetadataException, TeiidComponentException {
        this.modelID = modelID;
        this.metadata = metadata;
        this.capFinder = capFinder;
        this.caps = caps;
    }

    @Override
    public void visit(XMLAttributes obj) {
        if (willBecomeConstant(obj)) {
            return;
        }
        markInvalid(obj, "Pushdown of XMLAttributes not allowed"); //$NON-NLS-1$
    }

    private boolean willBecomeConstant(LanguageObject obj) {
        return checkEvaluation && EvaluatableVisitor.willBecomeConstant(obj, pushdown);
    }

    @Override
    public void visit(XMLNamespaces obj) {
        if (willBecomeConstant(obj)) {
            return;
        }
        markInvalid(obj, "Pushdown of XMLNamespaces not allowed"); //$NON-NLS-1$
    }

    @Override
    public void visit(TextLine obj) {
        markInvalid(obj, "Pushdown of TextLine not allowed"); //$NON-NLS-1$
    }

    @Override
    public void visit(XMLForest obj) {
        if (willBecomeConstant(obj)) {
            return;
        }
        markInvalid(obj, "Pushdown of XMLForest not allowed"); //$NON-NLS-1$
    }

    @Override
    public void visit(JSONObject obj) {
        if (willBecomeConstant(obj)) {
            return;
        }
        markInvalid(obj, "Pushdown of JSONObject not allowed"); //$NON-NLS-1$
    }

    @Override
    public void visit(XMLElement obj) {
        if (willBecomeConstant(obj)) {
            return;
        }
        markInvalid(obj, "Pushdown of XMLElement not allowed"); //$NON-NLS-1$
    }

    @Override
    public void visit(XMLSerialize obj) {
        if (willBecomeConstant(obj)) {
            return;
        }
        markInvalid(obj, "Pushdown of XMLSerialize not allowed"); //$NON-NLS-1$
    }

    @Override
    public void visit(XMLParse obj) {
        if (willBecomeConstant(obj)) {
            return;
        }
        markInvalid(obj, "Pushdown of XMLParse not allowed"); //$NON-NLS-1$
    }

    @Override
    public void visit(XMLQuery obj) {
        if (willBecomeConstant(obj)) {
            return;
        }
        markInvalid(obj, "Pushdown of XMLQuery not allowed"); //$NON-NLS-1$
    }

    @Override
    public void visit(XMLExists obj) {
        if (willBecomeConstant(obj)) {
            return;
        }
        markInvalid(obj, "Pushdown of XMLExists not allowed"); //$NON-NLS-1$
    }

    public void visit(XMLCast xmlCast) {
        if (willBecomeConstant(xmlCast)) {
            return;
        }
        markInvalid(xmlCast, "Pushdown of XMLCast not allowed"); //$NON-NLS-1$
    }

    @Override
    public void visit(QueryString obj) {
        if (willBecomeConstant(obj)) {
            return;
        }
        markInvalid(obj, "Pushdown of QueryString not allowed"); //$NON-NLS-1$
    }

    @Override
    public void visit(Array array) {
        try {
            if (!CapabilitiesUtil.supports(Capability.ARRAY_TYPE, modelID, metadata, capFinder)) {
                markInvalid(array, "Array type not supported by source"); //$NON-NLS-1$
            } else if (isSelectClause && !CapabilitiesUtil.supports(Capability.QUERY_SELECT_EXPRESSION_ARRAY_TYPE, modelID, metadata, capFinder)) {
                //TODO: this is just a workaround of sorts - as a comparison could be nested where this is allowed
                markInvalid(array, "Array type expression projection not supported by source"); //$NON-NLS-1$
            }
        } catch (QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);
        }
    }

    public void visit(AggregateSymbol obj) {
        try {
            if(! CapabilitiesUtil.supportsAggregateFunction(modelID, obj, metadata, capFinder)) {
                markInvalid(obj, "Aggregate function pushdown not supported by source"); //$NON-NLS-1$
            }
        } catch(QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);
        }
    }

    @Override
    public void visit(WindowFunction windowFunction) {
        if(! this.caps.supportsCapability(Capability.ELEMENTARY_OLAP)) {
            markInvalid(windowFunction, "Window function not supported by source"); //$NON-NLS-1$
            return;
        }
        if (!this.caps.supportsCapability(Capability.WINDOW_FUNCTION_ORDER_BY_AGGREGATES)
                && windowFunction.getWindowSpecification().getOrderBy() != null
                && !(windowFunction.getFunction().isAnalytical())) {
            markInvalid(windowFunction, "Window function order by with aggregate not supported by source"); //$NON-NLS-1$
            return;
        }
        if (!this.caps.supportsCapability(Capability.WINDOW_FUNCTION_DISTINCT_AGGREGATES)
                && windowFunction.getFunction().isDistinct()) {
            markInvalid(windowFunction, "Window function distinct aggregate not supported by source"); //$NON-NLS-1$
            return;
        }
        if (windowFunction.getWindowSpecification().getWindowFrame() != null) {
            if (!this.caps.supportsCapability(Capability.WINDOW_FUNCTION_FRAME_CLAUSE)) {
                markInvalid(windowFunction, "Window function frame clause not supported by source"); //$NON-NLS-1$
                return;
            }
        }
        /* Some sources do not like this case. While we don't allow it to be entered directly,
         * it can occur when raising a null node.
         * TODO: support rewrites of the ordering/entire window function expression
         */
        OrderBy orderBy = windowFunction.getWindowSpecification().getOrderBy();
        if (orderBy != null) {
            for (OrderByItem item : orderBy.getOrderByItems()) {
                if (EvaluatableVisitor.willBecomeConstant(SymbolMap.getExpression(item.getSymbol()))) {
                    markInvalid(windowFunction, "Window function order by constant not supported."); //$NON-NLS-1$
                    return;
                }
            }
        }
        try {
            if (!CapabilitiesUtil.checkElementsAreSearchable(windowFunction.getWindowSpecification().getPartition(), metadata, SupportConstants.Element.SEARCHABLE_COMPARE)) {
                markInvalid(windowFunction, "not all source columns support search type"); //$NON-NLS-1$
            }
        } catch(QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);
        }
    }

    @Override
    public void visit(OrderByItem obj) {
        try {
            checkElementsAreSearchable(obj.getSymbol(), SupportConstants.Element.SEARCHABLE_COMPARE);
            if (!CapabilitiesUtil.supportsNullOrdering(this.metadata, this.capFinder, this.modelID, obj)) {
                markInvalid(obj, "Desired null ordering is not supported by source"); //$NON-NLS-1$
            }
        } catch(QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);
        }
    }

    @Override
    public void visit(OrderBy obj) {
        String collation = null;
        try {
            collation = (String) CapabilitiesUtil.getProperty(Capability.COLLATION_LOCALE, modelID, metadata, capFinder);
        } catch(QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);
        }
        CommandContext commandContext = CommandContext.getThreadLocalContext();
        if (collation != null && commandContext != null && commandContext.getOptions().isRequireTeiidCollation() && !collation.equals(DataTypeManager.COLLATION_LOCALE)) {
            for (OrderByItem symbol : obj.getOrderByItems()) {
                if (symbol.getSymbol().getType() == DataTypeManager.DefaultDataClasses.STRING
                        || symbol.getSymbol().getType() == DataTypeManager.DefaultDataClasses.CLOB
                        || symbol.getSymbol().getType() == DataTypeManager.DefaultDataClasses.CHAR) {
                    //we require the collation to match
                    markInvalid(obj, "source is not using the same collation as Teiid"); //$NON-NLS-1$
                    break;
                }
            }
        }
    }

    public void visit(CaseExpression obj) {
        if(! this.caps.supportsCapability(Capability.QUERY_CASE) && !willBecomeConstant(obj)) {
            markInvalid(obj, "CaseExpression pushdown not supported by source"); //$NON-NLS-1$
        }
    }

    public void visit(CompareCriteria obj) {
        checkCompareCriteria(obj, obj.getRightExpression());
        checkLiteralComparison(obj, Arrays.asList(obj.getRightExpression()));
    }

    private void checkLiteralComparison(LanguageObject obj, Collection<? extends LanguageObject> toCheck) {
        if (isJoin || !this.caps.supportsCapability(Capability.CRITERIA_ONLY_LITERAL_COMPARE)) {
            return;
        }
        for (LanguageObject languageObject : toCheck) {
            if (!EvaluatableVisitor.willBecomeConstant(languageObject)) {
                markInvalid(obj, "Non-literal comparison not supported by source."); //$NON-NLS-1$
                return;
            }
        }
    }

    public void checkCompareCriteria(AbstractCompareCriteria obj, Expression rightExpression) {
        boolean negated = false;
        // Check if operation is allowed
        Capability operatorCap = null;
        switch(obj.getOperator()) {
            case CompareCriteria.NE:
                negated = true;
            case CompareCriteria.EQ:
                operatorCap = Capability.CRITERIA_COMPARE_EQ;
                break;
            case CompareCriteria.LT:
            case CompareCriteria.GT:
                operatorCap = Capability.CRITERIA_COMPARE_ORDERED_EXCLUSIVE;
                break;
            case CompareCriteria.LE:
            case CompareCriteria.GE:
                operatorCap = Capability.CRITERIA_COMPARE_ORDERED;
                break;
        }

        // Check if compares are allowed
        if(! this.caps.supportsCapability(operatorCap)) {
            boolean unsupported = true;
            if (operatorCap == Capability.CRITERIA_COMPARE_ORDERED_EXCLUSIVE
                    && this.caps.supportsCapability(Capability.CRITERIA_COMPARE_ORDERED) && this.caps.supportsCapability(Capability.CRITERIA_NOT)) {
                unsupported = false;
            }
            if (unsupported) {
                if (EvaluatableVisitor.willBecomeConstant(obj)) {
                    return;
                }
                markInvalid(obj, operatorCap + " CompareCriteria not supported by source"); //$NON-NLS-1$
                return;
            }
        }
        if (negated && !this.caps.supportsCapability(Capability.CRITERIA_NOT)) {
            if (EvaluatableVisitor.willBecomeConstant(obj)) {
                return;
            }
            markInvalid(obj, "Negation is not supported by source"); //$NON-NLS-1$
            return;
        }

        // Check capabilities of the elements
        try {
            int support = SupportConstants.Element.SEARCHABLE_COMPARE;
            if (!negated && obj.getOperator() == CompareCriteria.EQ) {
                support = SupportConstants.Element.SEARCHABLE_EQUALITY;
            }
            checkElementsAreSearchable(obj.getLeftExpression(), support);
            checkElementsAreSearchable(rightExpression, support);
        } catch(QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);
        }
    }

    public void visit(CompoundCriteria crit) {
        int operator = crit.getOperator();

        // Verify capabilities are supported
        if(operator == CompoundCriteria.OR && !this.caps.supportsCapability(Capability.CRITERIA_OR) && !willBecomeConstant(crit)) {
                markInvalid(crit, "OR criteria not supported by source"); //$NON-NLS-1$
        }
    }

    @Override
    public void visit(IsDistinctCriteria obj) {
        if (obj.getLeftRowValue() instanceof GroupSymbol || obj.getRightRowValue() instanceof GroupSymbol) {
            markInvalid(obj, "OR criteria not supported by source"); //$NON-NLS-1$
            return;
        }
        if (!this.caps.supportsCapability(Capability.CRITERIA_IS_DISTINCT)) {
            if (willBecomeConstant(obj)) {
                return;
            }
            markInvalid(obj, "OR criteria not supported by source"); //$NON-NLS-1$
            return;
        }
        //check NOT
        if(obj.isNegated() && ! this.caps.supportsCapability(Capability.CRITERIA_NOT)) {
            if (willBecomeConstant(obj)) {
                return;
            }
            markInvalid(obj, "Negation is not supported by source"); //$NON-NLS-1$
            return;
        }
        checkLiteralComparison(obj, Arrays.asList(obj.getRightRowValue()));
        try {
            int support = SupportConstants.Element.SEARCHABLE_COMPARE;
            if (!obj.isNegated()) {
                support = SupportConstants.Element.SEARCHABLE_EQUALITY;
            }
            checkElementsAreSearchable(obj.getLeftRowValue(), support);
            checkElementsAreSearchable(obj.getRightRowValue(), support);
        } catch(QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);
        }
    }

    static TreeSet<String> parseFormat = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);

    static {
        parseFormat.add(SourceSystemFunctions.PARSEBIGDECIMAL);
        parseFormat.add(SourceSystemFunctions.FORMATBIGDECIMAL);
        parseFormat.add(SourceSystemFunctions.PARSETIMESTAMP);
        parseFormat.add(SourceSystemFunctions.FORMATTIMESTAMP);
    }

    public void visit(Function obj) {
        try {
            if(obj.getFunctionDescriptor().getPushdown() == PushDown.CANNOT_PUSHDOWN) {
                //if the function can be evaluated then return as it will get replaced during the final rewrite
                if (willBecomeConstant(obj)) {
                    return;
                }
                markInvalid(obj, "Function metadata indicates it cannot be pusheddown."); //$NON-NLS-1$
                return;
            }
            if (! CapabilitiesUtil.supportsScalarFunction(modelID, obj, metadata, capFinder)) {
                //if the function can be evaluated then return as it will get replaced during the final rewrite
                if (willBecomeConstant(obj)) {
                    return;
                }
                markInvalid(obj, (obj.isImplicit()?"(implicit) ":"") + obj.getName() + " function not supported by source"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                return;
            }
            String name = obj.getName();
            if (caps.supportsCapability(Capability.ONLY_FORMAT_LITERALS) && parseFormat.contains(name)) {
                //if the function can be evaluated then return as it will get replaced during the final rewrite
                if (willBecomeConstant(obj)) {
                    return;
                }
                if (!(obj.getArg(1) instanceof Constant)) {
                    markInvalid(obj, obj.getName() + " non-literal parse format function not supported by source"); //$NON-NLS-1$
                    return;
                }
                Constant c = (Constant)obj.getArg(1);
                if (c.isMultiValued()) {
                    markInvalid(obj, obj.getName() + " non-literal parse format function not supported by source"); //$NON-NLS-1$
                    return;
                }
                if (!caps.supportsFormatLiteral((String)c.getValue(), StringUtil.endsWithIgnoreCase(name, DataTypeManager.DefaultDataTypes.TIMESTAMP)?Format.DATE:Format.NUMBER)) {
                    markInvalid(obj, obj.getName() + " literal parse " + c + " not supported by source"); //$NON-NLS-1$ //$NON-NLS-2$
                    return;
                }
                c.setBindEligible(false);
            }
            if (name.equalsIgnoreCase(SourceSystemFunctions.TIMESTAMPADD)) {
                if (caps.supportsCapability(Capability.ONLY_TIMESTAMPADD_LITERAL)) {
                    //if the function can be evaluated then return as it will get replaced during the final rewrite
                    if (willBecomeConstant(obj)) {
                        return;
                    }
                    if (!(obj.getArg(1) instanceof Constant)) {
                        markInvalid(obj, obj.getName() + " non-literal timestampadd not supported by source"); //$NON-NLS-1$
                        return;
                    }
                    Constant c = (Constant)obj.getArg(1);
                    if (c.isMultiValued()) {
                        markInvalid(obj, obj.getName() + " non-literal timestampadd function not supported by source"); //$NON-NLS-1$
                        return;
                    }
                    c.setBindEligible(false);
                }
                if (obj.getArg(1).getType() == DataTypeManager.DefaultDataClasses.LONG
                        && !willBecomeConstant(obj.getArg(1))
                        && !(caps.supportsFunction(SourceSystemFunctions.CONVERT) && caps.supportsConvert(DataTypeManager.DefaultTypeCodes.LONG, DataTypeManager.DefaultTypeCodes.INTEGER))) {
                    markInvalid(obj, obj.getName() + " cannot narrow long argument timestampadd argument to int"); //$NON-NLS-1$
                    return;
                }
            }
        } catch(QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);
        }
    }

    public void visit(IsNullCriteria obj) {
        // Check if compares are allowed
        if(! this.caps.supportsCapability(Capability.CRITERIA_ISNULL)) {
            if (willBecomeConstant(obj)) {
                return;
            }
            markInvalid(obj, "IsNull not supported by source"); //$NON-NLS-1$
            return;
        }

        if (obj.isNegated() && !this.caps.supportsCapability(Capability.CRITERIA_NOT)) {
            if (willBecomeConstant(obj)) {
                return;
            }
            markInvalid(obj, "Negation is not supported by source"); //$NON-NLS-1$
            return;
        }
    }

    public void visit(MatchCriteria obj) {
        switch (obj.getMode()) {
        case LIKE:
            if(! this.caps.supportsCapability(Capability.CRITERIA_LIKE)) {
                if (willBecomeConstant(obj)) {
                    return;
                }
                markInvalid(obj, "Like is not supported by source"); //$NON-NLS-1$
                return;
            }
            break;
        case SIMILAR:
            if(! this.caps.supportsCapability(Capability.CRITERIA_SIMILAR)) {
                if (willBecomeConstant(obj)) {
                    return;
                }
                markInvalid(obj, "Similar to is not supported by source"); //$NON-NLS-1$
                return;
            }
            break;
        case REGEX:
            if(! this.caps.supportsCapability(Capability.CRITERIA_LIKE_REGEX)) {
                if (willBecomeConstant(obj)) {
                    return;
                }
                markInvalid(obj, "Like_regex is not supported by source"); //$NON-NLS-1$
                return;
            }
            break;
        }

        // Check ESCAPE char if necessary
        if(obj.getEscapeChar() != MatchCriteria.NULL_ESCAPE_CHAR
                && ! this.caps.supportsCapability(Capability.CRITERIA_LIKE_ESCAPE)) {
            if (willBecomeConstant(obj)) {
                return;
            }
            markInvalid(obj, "Like escape is not supported by source"); //$NON-NLS-1$
            return;
        }

        Character required = (Character) caps.getSourceProperty(Capability.REQUIRED_LIKE_ESCAPE);
        if (required != null
                && obj.getEscapeChar() != MatchCriteria.NULL_ESCAPE_CHAR
                    && !required.equals(obj.getEscapeChar())) {
            if (willBecomeConstant(obj)) {
                return;
            }
            markInvalid(obj, "Escape " + obj.getEscapeChar() + " is not supported by source. Escape " + required + " is required"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            return;
        }

        //check NOT
        if(obj.isNegated() && ! this.caps.supportsCapability(Capability.CRITERIA_NOT)) {
            if (willBecomeConstant(obj)) {
                return;
            }
            markInvalid(obj, "Negation is not supported by source"); //$NON-NLS-1$
            return;
        }

        // Check capabilities of the elements
        try {
            checkElementsAreSearchable(obj.getLeftExpression(), SupportConstants.Element.SEARCHABLE_LIKE);
        } catch(QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);
        }

        checkLiteralComparison(obj, Arrays.asList(obj.getRightExpression()));
    }

    public void visit(NotCriteria obj) {
        // Check if compares are allowed
        if(! this.caps.supportsCapability(Capability.CRITERIA_NOT) && !willBecomeConstant(obj)) {
            markInvalid(obj, "Negation is not supported by source"); //$NON-NLS-1$
            return;
        }
    }

    public void visit(SearchedCaseExpression obj) {
        if(! this.caps.supportsCapability(Capability.QUERY_SEARCHED_CASE) && !willBecomeConstant(obj)) {
            markInvalid(obj, "SearchedCase is not supported by source"); //$NON-NLS-1$
        }
    }

    public void visit(SetCriteria crit) {
        checkAbstractSetCriteria(crit);
        try {
            int maxSize = CapabilitiesUtil.getMaxInCriteriaSize(modelID, metadata, capFinder);
            int maxPredicates = CapabilitiesUtil.getMaxDependentPredicates(modelID, metadata, capFinder);
            //allow 1/2 to a single predicate - TODO: make this more precise
            if (maxSize > 0 && maxPredicates > 0 && crit.getValues().size() > Math.max(maxSize, (maxSize * (long)maxPredicates)/2)) {
                if (willBecomeConstant(crit)) {
                    return;
                }
                markInvalid(crit, "SetCriteria size exceeds maximum for source"); //$NON-NLS-1$
                return;
            }
        } catch(QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);
        }
        checkLiteralComparison(crit, crit.getValues());
    }

    /**
     * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.lang.ExistsCriteria)
     */
    public void visit(ExistsCriteria crit) {
        if (crit.shouldEvaluate()) {
            return;
        }
        boolean canPreEval = crit.getCommand().getCorrelatedReferences() == null;
        // Check if exists criteria are allowed
        if(! this.caps.supportsCapability(Capability.CRITERIA_EXISTS)) {
            markEvaluatable(crit, canPreEval, "Exists is not supported by source"); //$NON-NLS-1$
        } else if (crit.isNegated() && !this.caps.supportsCapability(Capability.CRITERIA_NOT)) {
            markEvaluatable(crit, canPreEval, "Negation is not supported by source"); //$NON-NLS-1$
        } else {
            try {
                if (validateSubqueryPushdown(crit, modelID, metadata, capFinder, analysisRecord) == null) {
                    markEvaluatable(crit, canPreEval, "Subquery cannot be pushed down"); //$NON-NLS-1$
                }
            } catch (TeiidComponentException e) {
                handleException(e);
            }
        }
    }

    /**
     * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.lang.SubqueryCompareCriteria)
     */
    public void visit(SubqueryCompareCriteria crit) {
        if (crit.getArrayExpression() != null) {
            markInvalid(crit, "Quantified compare with an array cannot yet be pushed down."); //$NON-NLS-1$
            return;
        }
        // Check if quantification operator is allowed
        Capability capability = Capability.QUERY_SUBQUERIES_SCALAR;
        switch(crit.getPredicateQuantifier()) {
            case SubqueryCompareCriteria.ALL:
                capability = Capability.CRITERIA_QUANTIFIED_ALL;
                break;
            case SubqueryCompareCriteria.ANY:
                capability = Capability.CRITERIA_QUANTIFIED_SOME;
                break;
            case SubqueryCompareCriteria.SOME:
                capability = Capability.CRITERIA_QUANTIFIED_SOME;
                break;
        }
        if(! this.caps.supportsCapability(capability)) {
            markInvalid(crit, "SubqueryCompare not supported by source"); //$NON-NLS-1$
            return;
        }

        checkCompareCriteria(crit, crit.getCommand().getProjectedSymbols().get(0));

        // Check capabilities of the elements
        try {
            if (validateSubqueryPushdown(crit, modelID, metadata, capFinder, analysisRecord) == null) {
                markInvalid(crit.getCommand(), "Subquery cannot be pushed down"); //$NON-NLS-1$
            }
        } catch(QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);
        }
    }

    @Override
    public void visit(ScalarSubquery obj) {
        try {
            if (obj.shouldEvaluate()) {
                return;
            }
            boolean canPreEval = obj.getCommand().getCorrelatedReferences() == null;
            if(!this.caps.supportsCapability(Capability.QUERY_SUBQUERIES_SCALAR)) {
                markEvaluatable(obj, canPreEval, "Correlated/nonDeterministic ScalarSubquery is not supported"); //$NON-NLS-1$
            } else if (validateSubqueryPushdown(obj, modelID, metadata, capFinder, analysisRecord) == null) {
                markEvaluatable(obj, canPreEval, "Subquery cannot be pushed down"); //$NON-NLS-1$
            } else if (this.isSelectClause && !this.caps.supportsCapability(Capability.QUERY_SUBQUERIES_SCALAR_PROJECTION)) {
                markEvaluatable(obj, canPreEval, "Correlated/nonDeterministic ScalarSubquery cannot be used in the SELECT clause"); //$NON-NLS-1$
            } else if (canPreEval) {
                //preserve the prior behavior of always pre-evaluating against a with temp group
                Collection<GroupSymbol> groups = GroupCollectorVisitor.getGroupsIgnoreInlineViews(obj.getCommand(), true);
                boolean allTemp = true;
                for (GroupSymbol gs : groups) {
                    if (!gs.isPushedCommonTable()) {
                        allTemp = false;
                        break;
                    }
                }
                if (allTemp) {
                    obj.setShouldEvaluate(true);
                }
            }
        } catch(QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);
        }
    }

    private void markEvaluatable(SubqueryContainer.Evaluatable<QueryCommand> obj, boolean canPreEval, String reasonWhyInvalid) {
        if (canPreEval) {
            obj.setShouldEvaluate(true);
        } else {
            markInvalid(obj.getCommand(), reasonWhyInvalid);
        }
    }

    public void visit(SubquerySetCriteria crit) {
        checkAbstractSetCriteria(crit);
        try {
            // Check if compares with subqueries are allowed
            if(! this.caps.supportsCapability(Capability.CRITERIA_IN_SUBQUERY)) {
                markInvalid(crit, "SubqueryIn is not supported by source"); //$NON-NLS-1$
                return;
            }

            if (validateSubqueryPushdown(crit, modelID, metadata, capFinder, analysisRecord) == null) {
                markInvalid(crit.getCommand(), "Subquery cannot be pushed down"); //$NON-NLS-1$
            }
        } catch(QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);
        }
    }

    public void checkAbstractSetCriteria(AbstractSetCriteria crit) {
        try {
            // Check if compares are allowed
            if(! this.caps.supportsCapability(Capability.CRITERIA_IN)) {
                if (willBecomeConstant(crit)) {
                    return;
                }
                markInvalid(crit, "In is not supported by source"); //$NON-NLS-1$
                return;
            }

            if (crit.isNegated() && !this.caps.supportsCapability(Capability.CRITERIA_NOT)) {
                if (willBecomeConstant(crit)) {
                    return;
                }
                markInvalid(crit, "Negation is not supported by source"); //$NON-NLS-1$
                return;
            }
            // Check capabilities of the elements
            checkElementsAreSearchable(crit.getExpression(), SupportConstants.Element.SEARCHABLE_COMPARE);

        } catch(QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);
        }

    }

    public void visit(DependentSetCriteria crit) {
        checkAbstractSetCriteria(crit);
    }

    private void checkElementsAreSearchable(LanguageObject crit, int searchableType)
    throws QueryMetadataException, TeiidComponentException {
        if (!CapabilitiesUtil.checkElementsAreSearchable(Arrays.asList(crit), metadata, searchableType)) {
            if (willBecomeConstant(crit)) {
                return;
            }
            markInvalid(crit, "not all source columns support search type"); //$NON-NLS-1$
        }
    }

    /**
     * Return null if the subquery cannot be pushed down, otherwise the model
     * id of the pushdown target.
     * @param subqueryContainer
     * @param critNodeModelID
     * @param metadata
     * @param capFinder
     * @return
     * @throws TeiidComponentException
     */
    public static Object validateSubqueryPushdown(SubqueryContainer<?> subqueryContainer, Object critNodeModelID,
            QueryMetadataInterface metadata, CapabilitiesFinder capFinder, AnalysisRecord analysisRecord) throws TeiidComponentException {
        ProcessorPlan plan = subqueryContainer.getCommand().getProcessorPlan();
        if (plan != null) {
            AccessNode aNode = getAccessNode(plan);

            if (aNode == null) {
                return null;
            }

            critNodeModelID = validateCommandPushdown(critNodeModelID, metadata, capFinder,    aNode, true);
        }
        if (critNodeModelID == null) {
            return null;
        }
        // Check whether source supports correlated subqueries and if not, whether criteria has them
        SymbolMap refs = subqueryContainer.getCommand().getCorrelatedReferences();
        try {
            if(refs != null && !refs.asMap().isEmpty()) {
                if(! CapabilitiesUtil.supports(Capability.QUERY_SUBQUERIES_CORRELATED, critNodeModelID, metadata, capFinder)) {
                    return null;
                }
                if (!CapabilitiesUtil.supports(Capability.SUBQUERY_CORRELATED_LIMIT, critNodeModelID, metadata, capFinder)) {
                    QueryCommand command = (QueryCommand)subqueryContainer.getCommand();
                    if (command.getLimit() != null && !command.getLimit().isImplicit()) {
                        return null;
                    }
                }
                //TODO: this check sees as correlated references as coming from the containing scope
                //but this is only an issue with deeply nested subqueries
                //we set the extra validation option to indicate that this is a full pushdown, rather than something
                //that will be evaluated
                if (!CriteriaCapabilityValidatorVisitor.canPushLanguageObject(subqueryContainer.getCommand(), critNodeModelID, metadata, capFinder, analysisRecord,
                        new ValidatorOptions().pushdown(true) )) {
                    return null;
                }
            } else if (CapabilitiesUtil.supports(Capability.QUERY_SUBQUERIES_ONLY_CORRELATED, critNodeModelID, metadata, capFinder)) {
                return null;
            }
        } catch(QueryMetadataException e) {
             throw new TeiidComponentException(QueryPlugin.Event.TEIID30271, e);
        }

        if (!CapabilitiesUtil.supports(Capability.SUBQUERY_COMMON_TABLE_EXPRESSIONS, critNodeModelID, metadata, capFinder)
                && subqueryContainer.getCommand() instanceof QueryCommand) {
            QueryCommand command = (QueryCommand)subqueryContainer.getCommand();
            if (command.getWith() != null) {
                return null;
            }
        }

        // Found no reason why this node is not eligible
        return critNodeModelID;
    }

    public static Object validateCommandPushdown(Object critNodeModelID,
            QueryMetadataInterface metadata, CapabilitiesFinder capFinder,
            AccessNode aNode, boolean considerConformed) throws TeiidComponentException {
        // Check that query in access node is for the same model as current node
        try {
            if (!(aNode.getCommand() instanceof QueryCommand)) {
                return null;
            }
            Object modelID = aNode.getModelId();
            if (critNodeModelID == null) {
                critNodeModelID = modelID;
            } else if(!CapabilitiesUtil.isSameConnector(critNodeModelID, modelID, metadata, capFinder)
                    && (!considerConformed || !RuleRaiseAccess.isConformed(metadata, capFinder, aNode.getConformedTo(), modelID, null, critNodeModelID))) {
                return null;
            }
        } catch(QueryMetadataException e) {
             throw new TeiidComponentException(QueryPlugin.Event.TEIID30272, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30272));
        }
        return critNodeModelID;
    }

    public static AccessNode getAccessNode(ProcessorPlan plan) {
        if(!(plan instanceof RelationalPlan)) {
            return null;
        }

        RelationalPlan rplan = (RelationalPlan) plan;

        // Check that the plan is just an access node
        RelationalNode accessNode = rplan.getRootNode();

        if (accessNode instanceof LimitNode) {
            LimitNode ln = (LimitNode)accessNode;
            if (!ln.isImplicit()) {
                return null;
            }
            accessNode = ln.getChildren()[0];
        }

        if (! (accessNode instanceof AccessNode)) {
            return null;
        }
        return (AccessNode)accessNode;
    }

    public static QueryCommand getQueryCommand(AccessNode aNode) {
        if (aNode == null) {
            return null;
        }
        Command command = aNode.getCommand();
        if(!(command instanceof QueryCommand)) {
            return null;
        }

        QueryCommand queryCommand = (QueryCommand)command;
        if (aNode.getProjection() != null && aNode.getProjection().length > 0) {
            Query newCommand = (Query)queryCommand.clone();
            newCommand.getSelect().setSymbols(aNode.getOriginalSelect());
            return newCommand;
        }
        return queryCommand;
    }

    private void handleException(TeiidComponentException e) {
        this.valid = false;
        this.exception = e;
        setAbort(true);
    }

    public TeiidComponentException getException() {
        return this.exception;
    }

    private void markInvalid(LanguageObject object, String reason) {
        this.valid = false;
        setAbort(true);
        if (analysisRecord != null && analysisRecord.recordAnnotations()) {
            try {
                analysisRecord.addAnnotation(Annotation.RELATIONAL_PLANNER, reason + " " + this.metadata.getName(this.modelID), object + " was not pushed", Priority.LOW); //$NON-NLS-1$ //$NON-NLS-2$
            } catch (QueryMetadataException e) {
            } catch (TeiidComponentException e) {
            }
        }
    }

    public boolean isValid() {
        return this.valid;
    }

    public static boolean canPushLanguageObject(LanguageObject obj, Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, AnalysisRecord analysisRecord) throws QueryMetadataException, TeiidComponentException {
        return canPushLanguageObject(obj, modelID, metadata, capFinder, analysisRecord, new ValidatorOptions());
    }

    public static boolean canPushLanguageObject(LanguageObject obj, Object modelID, final QueryMetadataInterface metadata, CapabilitiesFinder capFinder, AnalysisRecord analysisRecord, ValidatorOptions parameterObject) throws QueryMetadataException, TeiidComponentException {
        if(obj == null) {
            return true;
        }

        if(modelID == null || metadata.isVirtualModel(modelID)) {
            // Couldn't determine model ID, so give up
            return false;
        }

        String modelName = metadata.getFullName(modelID);
        SourceCapabilities caps = capFinder.findCapabilities(modelName);

        if (caps == null) {
            return true; //this doesn't seem right, but tests were expecting it...
        }

        CriteriaCapabilityValidatorVisitor visitor = new CriteriaCapabilityValidatorVisitor(modelID, metadata, capFinder, caps);
        visitor.setCheckEvaluation(!parameterObject.multiValuedReferences);
        visitor.analysisRecord = analysisRecord;
        visitor.isJoin = parameterObject.isJoin;
        visitor.isSelectClause = parameterObject.isSelectClause;
        visitor.pushdown = parameterObject.pushdown;
        //we use an array to represent multiple comparison attributes,
        //but we don't want that to inhibit pushdown as we'll account for that later
        //in criteria processing
        final EvaluatableVisitor ev = new EvaluatableVisitor(modelID, metadata, capFinder);
        PreOrPostOrderNavigator nav = new PreOrPostOrderNavigator(visitor, PreOrPostOrderNavigator.POST_ORDER, false) {
            @Override
            public void visit(DependentSetCriteria obj1) {
                if (obj1.hasMultipleAttributes() && obj1.getExpression() instanceof Array) {
                    Array array = (Array) obj1.getExpression();
                    visitNodes(array.getExpressions());
                    super.postVisitVisitor(obj1);
                } else {
                    super.visit(obj1);
                }
            }

            @Override
            protected void visitNode(LanguageObject obj) {
                if (obj == null) {
                    return;
                }
                Determinism d = ev.getDeterminismLevel();
                boolean pushDown = ev.requiresEvaluation(EvaluationLevel.PUSH_DOWN);
                //descend with clean state, then restore
                ev.reset();
                super.visitNode(obj);
                ev.setDeterminismLevel(d);
                if (pushDown) {
                    ev.evaluationNotPossible(EvaluationLevel.PUSH_DOWN);
                }
            }

            @Override
            protected void visitVisitor(LanguageObject obj) {
                if (obj == null) {
                    return;
                }
                if (!ev.requiresEvaluation(EvaluationLevel.PUSH_DOWN)
                        && ev.getDeterminismLevel() != Determinism.NONDETERMINISTIC) {
                    if (obj instanceof ElementSymbol) {
                        ElementSymbol es = (ElementSymbol)obj;
                        if (es.getMetadataID() != null) {
                            try {
                                if (metadata.isMultiSourceElement(es.getMetadataID())) {
                                    return;  //no need to visit
                                }
                            } catch (QueryMetadataException e) {
                            } catch (TeiidComponentException e) {
                            }
                        }
                    }
                    obj.acceptVisitor(ev);
                    if (!parameterObject.multiValuedReferences && obj instanceof Expression) {
                        if (obj instanceof Function) {
                            if (!(obj instanceof AggregateSymbol)) {
                                Function f = (Function)obj;
                                if (f.getFunctionDescriptor().getPushdown() != PushDown.MUST_PUSHDOWN
                                        && f.getFunctionDescriptor().getDeterministic() != Determinism.NONDETERMINISTIC) {
                                    return; //don't need to consider
                                }
                            }
                        } else if (obj instanceof Criteria
                                && !(obj instanceof SubqueryContainer)
                                && !(obj instanceof DependentSetCriteria)) {
                            return; //don't need to consider
                        }
                    }
                }
                super.visitVisitor(obj);
            }
        };
        obj.acceptVisitor(nav);

        if(visitor.getException() != null) {
            throw visitor.getException();
        }

        return visitor.isValid();
    }

    public void setCheckEvaluation(boolean b) {
        this.checkEvaluation = b;
    }

}
