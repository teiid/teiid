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

package org.teiid.query.sql.navigator;

import java.util.Collection;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.proc.*;
import org.teiid.query.sql.symbol.*;



/**
 * @since 4.2
 */
public class PreOrPostOrderNavigator extends AbstractNavigator {

    public static final boolean PRE_ORDER = true;
    public static final boolean POST_ORDER = false;

    private boolean order;
    private boolean deep;
    private boolean skipEvaluatable;

    public PreOrPostOrderNavigator(LanguageVisitor visitor, boolean order, boolean deep) {
        super(visitor);
        this.order = order;
        this.deep = deep;
    }

    protected void preVisitVisitor(LanguageObject obj) {
        if(order == PRE_ORDER) {
            visitVisitor(obj);
        }
    }

    protected void postVisitVisitor(LanguageObject obj) {
        if(order == POST_ORDER) {
            visitVisitor(obj);
        }
    }

    public void visit(AggregateSymbol obj) {
        preVisitVisitor(obj);
        Expression[] args = obj.getArgs();
        if(args != null) {
            for(int i=0; i<args.length; i++) {
                visitNode(args[i]);
            }
        }
        visitNode(obj.getOrderBy());
        visitNode(obj.getCondition());
        postVisitVisitor(obj);
    }
    public void visit(AliasSymbol obj) {
        preVisitVisitor(obj);
        visitNode(obj.getSymbol());
        postVisitVisitor(obj);
    }
    public void visit(MultipleElementSymbol obj) {
        preVisitVisitor(obj);
        postVisitVisitor(obj);
    }
    public void visit(AssignmentStatement obj) {
        preVisitVisitor(obj);
        visitNode(obj.getVariable());
        visitNode(obj.getExpression());
        postVisitVisitor(obj);
    }
    public void visit(BatchedUpdateCommand obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getUpdateCommands());
        postVisitVisitor(obj);
    }
    public void visit(BetweenCriteria obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        visitNode(obj.getLowerExpression());
        visitNode(obj.getUpperExpression());
        postVisitVisitor(obj);
    }
    public void visit(Block obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getStatements());
        visitNodes(obj.getExceptionStatements());
        postVisitVisitor(obj);
    }
    public void visit(BranchingStatement obj) {
        preVisitVisitor(obj);
        postVisitVisitor(obj);
    }
    public void visit(CaseExpression obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        for(int i=0; i<obj.getWhenCount(); i++) {
            visitNode(obj.getWhenExpression(i));
            visitNode(obj.getThenExpression(i));
        }
        visitNode(obj.getElseExpression());
        postVisitVisitor(obj);
    }
    public void visit(CommandStatement obj) {
        preVisitVisitor(obj);
        if (deep) {
            visitNode(obj.getCommand());
        }
        postVisitVisitor(obj);
    }
    public void visit(CompareCriteria obj) {
        preVisitVisitor(obj);
        visitNode(obj.getLeftExpression());
        visitNode(obj.getRightExpression());
        postVisitVisitor(obj);
    }
    public void visit(CompoundCriteria obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getCriteria());
        postVisitVisitor(obj);
    }
    public void visit(Constant obj) {
        preVisitVisitor(obj);
        postVisitVisitor(obj);
    }
    public void visit(CreateProcedureCommand obj) {
        preVisitVisitor(obj);
        visitNode(obj.getBlock());
        postVisitVisitor(obj);
    }
    public void visit(DeclareStatement obj) {
        preVisitVisitor(obj);
        visitNode(obj.getVariable());
        visitNode(obj.getExpression());
        postVisitVisitor(obj);
    }
    public void visit(Delete obj) {
        preVisitVisitor(obj);
        visitNode(obj.getGroup());
        visitNode(obj.getCriteria());
        visitNode(obj.getOption());
        postVisitVisitor(obj);
    }
    public void visit(DependentSetCriteria obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        postVisitVisitor(obj);
    }
    public void visit(ElementSymbol obj) {
        preVisitVisitor(obj);
        postVisitVisitor(obj);
    }
    public void visit(ExistsCriteria obj) {
        preVisitVisitor(obj);
        if (deep && (!obj.shouldEvaluate() || !skipEvaluatable)) {
            visitNode(obj.getCommand());
        }
        postVisitVisitor(obj);
    }
    public void visit(ExpressionSymbol obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        postVisitVisitor(obj);
    }
    public void visit(From obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getClauses());
        postVisitVisitor(obj);
    }
    public void visit(Function obj) {
        preVisitVisitor(obj);
        Expression[] args = obj.getArgs();
        if(args != null) {
            for(int i=0; i<args.length; i++) {
                visitNode(args[i]);
            }
        }
        postVisitVisitor(obj);
    }
    public void visit(GroupBy obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getSymbols());
        postVisitVisitor(obj);
    }
    public void visit(GroupSymbol obj) {
        preVisitVisitor(obj);
        postVisitVisitor(obj);
    }
    public void visit(IfStatement obj) {
        preVisitVisitor(obj);
        visitNode(obj.getCondition());
        visitNode(obj.getIfBlock());
        visitNode(obj.getElseBlock());
        postVisitVisitor(obj);
    }
    public void visit(Insert obj) {
        preVisitVisitor(obj);
        visitNode(obj.getGroup());
        visitNodes(obj.getVariables());
        visitNodes(obj.getValues());
        if(deep && obj.getQueryExpression()!=null) {
            visitNode(obj.getQueryExpression());
        }
        visitNode(obj.getOption());
        postVisitVisitor(obj);
    }
    public void visit(Create obj) {
        preVisitVisitor(obj);
        visitNode(obj.getTable());
        visitNodes(obj.getColumnSymbols());
        visitNodes(obj.getPrimaryKey());
        postVisitVisitor(obj);
    }
    public void visit(Drop obj) {
        preVisitVisitor(obj);
        visitNode(obj.getTable());
        postVisitVisitor(obj);
    }
    public void visit(Into obj) {
        preVisitVisitor(obj);
        visitNode(obj.getGroup());
        postVisitVisitor(obj);
    }
    public void visit(IsNullCriteria obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        postVisitVisitor(obj);
    }
    public void visit(JoinPredicate obj) {
        preVisitVisitor(obj);
        visitNode(obj.getLeftClause());
        visitNode(obj.getJoinType());
        visitNode(obj.getRightClause());
        visitNodes(obj.getJoinCriteria());
        postVisitVisitor(obj);
    }
    public void visit(JoinType obj) {
        preVisitVisitor(obj);
        postVisitVisitor(obj);
    }
    public void visit(Limit obj) {
        preVisitVisitor(obj);
        visitNode(obj.getOffset());
        visitNode(obj.getRowLimit());
        postVisitVisitor(obj);
    }
    public void visit(LoopStatement obj) {
        preVisitVisitor(obj);
        if (deep) {
            visitNode(obj.getCommand());
        }
        visitNode(obj.getBlock());
        postVisitVisitor(obj);
    }
    public void visit(MatchCriteria obj) {
        preVisitVisitor(obj);
        visitNode(obj.getLeftExpression());
        visitNode(obj.getRightExpression());
        postVisitVisitor(obj);
    }
    public void visit(NotCriteria obj) {
        preVisitVisitor(obj);
        visitNode(obj.getCriteria());
        postVisitVisitor(obj);
    }
    public void visit(Option obj) {
        preVisitVisitor(obj);
        postVisitVisitor(obj);
    }
    public void visit(OrderBy obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getOrderByItems());
        postVisitVisitor(obj);
    }
    @Override
    public void visit(OrderByItem obj) {
        preVisitVisitor(obj);
        visitNode(obj.getSymbol());
        postVisitVisitor(obj);
    }
    public void visit(Query obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getWith());
        visitNode(obj.getSelect());
        visitNode(obj.getInto());
        visitNode(obj.getFrom());
        visitNode(obj.getCriteria());
        visitNode(obj.getGroupBy());
        visitNode(obj.getHaving());
        visitNode(obj.getOrderBy());
        visitNode(obj.getLimit());
        visitNode(obj.getOption());
        postVisitVisitor(obj);
    }
    public void visit(RaiseStatement obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        postVisitVisitor(obj);
    }
    public void visit(Reference obj) {
        preVisitVisitor(obj);
        postVisitVisitor(obj);
    }
    public void visit(ScalarSubquery obj) {
        preVisitVisitor(obj);
        if (deep && (!obj.shouldEvaluate() || !skipEvaluatable)) {
            visitNode(obj.getCommand());
        }
        postVisitVisitor(obj);
    }
    public void visit(SearchedCaseExpression obj) {
        preVisitVisitor(obj);
        for(int i=0; i<obj.getWhenCount(); i++) {
            visitNode(obj.getWhenCriteria(i));
            visitNode(obj.getThenExpression(i));
        }
        visitNode(obj.getElseExpression());
        postVisitVisitor(obj);
    }
    public void visit(Select obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getSymbols());
        postVisitVisitor(obj);
    }
    public void visit(SetCriteria obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        visitNodes(obj.getValues());
        postVisitVisitor(obj);
    }
    public void visit(SetQuery obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getWith());
        visitNodes(obj.getQueryCommands());
        visitNode(obj.getOrderBy());
        visitNode(obj.getLimit());
        visitNode(obj.getOption());
        postVisitVisitor(obj);
    }

    public void visit(StoredProcedure obj) {
        preVisitVisitor(obj);

        Collection<SPParameter> params = obj.getParameters();
        if(params != null && !params.isEmpty()) {
            for (SPParameter parameter : params) {
                Expression expression = parameter.getExpression();
                visitNode(expression);
            }
        }

        visitNode(obj.getOption());
        postVisitVisitor(obj);
    }
    public void visit(SubqueryCompareCriteria obj) {
        preVisitVisitor(obj);
        visitNode(obj.getLeftExpression());
        if (deep) {
            visitNode(obj.getCommand());
        }
        visitNode(obj.getArrayExpression());
        postVisitVisitor(obj);
    }
    public void visit(SubqueryFromClause obj) {
        preVisitVisitor(obj);
        if (deep) {
            visitNode(obj.getCommand());
        }
        visitNode(obj.getGroupSymbol());
        postVisitVisitor(obj);
    }
    public void visit(SubquerySetCriteria obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        if (deep) {
            visitNode(obj.getCommand());
        }
        postVisitVisitor(obj);
    }
    public void visit(UnaryFromClause obj) {
        preVisitVisitor(obj);
        visitNode(obj.getGroup());
        postVisitVisitor(obj);
    }
    public void visit(Update obj) {
        preVisitVisitor(obj);
        visitNode(obj.getGroup());
        visitNode(obj.getChangeList());
        visitNode(obj.getCriteria());
        visitNode(obj.getOption());
        postVisitVisitor(obj);
    }
    public void visit(WhileStatement obj) {
        preVisitVisitor(obj);
        visitNode(obj.getCondition());
        visitNode(obj.getBlock());
        postVisitVisitor(obj);
    }

    /**
     * NOTE: we specifically don't need to visit the as columns or the using identifiers.
     * These will be resolved by the dynamic command resolver instead.
     *
     * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.lang.DynamicCommand)
     */
    public void visit(DynamicCommand obj) {
        preVisitVisitor(obj);
        visitNode(obj.getSql());
        visitNode(obj.getIntoGroup());
        if (obj.getUsing() != null) {
            for (SetClause setClause : obj.getUsing().getClauses()) {
                visitNode(setClause.getValue());
            }
        }
        postVisitVisitor(obj);
    }

    public void visit(SetClauseList obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getClauses());
        postVisitVisitor(obj);
    }

    public void visit(SetClause obj) {
        preVisitVisitor(obj);
        visitNode(obj.getSymbol());
        visitNode(obj.getValue());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(TextLine obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getExpressions());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(XMLForest obj) {
        preVisitVisitor(obj);
        visitNode(obj.getNamespaces());
        visitNodes(obj.getArgs());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(JSONObject obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getArgs());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(XMLAttributes obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getArgs());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(XMLElement obj) {
        preVisitVisitor(obj);
        visitNode(obj.getNamespaces());
        visitNode(obj.getAttributes());
        visitNodes(obj.getContent());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(XMLNamespaces obj) {
        preVisitVisitor(obj);
        postVisitVisitor(obj);
    }

    @Override
    public void visit(TextTable obj) {
        preVisitVisitor(obj);
        visitNode(obj.getFile());
        visitNode(obj.getGroupSymbol());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(XMLTable obj) {
        preVisitVisitor(obj);
        visitNode(obj.getNamespaces());
        visitNodes(obj.getPassing());
        for (XMLTable.XMLColumn column : obj.getColumns()) {
            visitNode(column.getDefaultExpression());
        }
        visitNode(obj.getGroupSymbol());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(JsonTable obj) {
        preVisitVisitor(obj);
        visitNode(obj.getJson());
        visitNode(obj.getGroupSymbol());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(ObjectTable obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getPassing());
        for (ObjectTable.ObjectColumn column : obj.getColumns()) {
            visitNode(column.getDefaultExpression());
        }
        visitNode(obj.getGroupSymbol());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(XMLQuery obj) {
        preVisitVisitor(obj);
        visitNode(obj.getNamespaces());
        visitNodes(obj.getPassing());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(XMLExists obj) {
        preVisitVisitor(obj);
        visitNode(obj.getXmlQuery().getNamespaces());
        visitNodes(obj.getXmlQuery().getPassing());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(XMLCast obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(DerivedColumn obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(XMLSerialize obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(QueryString obj) {
        preVisitVisitor(obj);
        visitNode(obj.getPath());
        visitNodes(obj.getArgs());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(XMLParse obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(ExpressionCriteria obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(WithQueryCommand obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getColumns());
        if (deep) {
            visitNode(obj.getCommand());
        }
        postVisitVisitor(obj);
    }

    @Override
    public void visit(TriggerAction obj) {
        preVisitVisitor(obj);
        visitNode(obj.getBlock());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(ArrayTable obj) {
        preVisitVisitor(obj);
        visitNode(obj.getArrayValue());
        visitNode(obj.getGroupSymbol());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(AlterProcedure obj) {
        preVisitVisitor(obj);
        visitNode(obj.getTarget());
        if (deep) {
            visitNode(obj.getDefinition());
        }
        postVisitVisitor(obj);
    }

    @Override
    public void visit(AlterTrigger obj) {
        preVisitVisitor(obj);
        visitNode(obj.getTarget());
        if (deep) {
            visitNode(obj.getDefinition());
        }
        postVisitVisitor(obj);
    }

    @Override
    public void visit(AlterView obj) {
        preVisitVisitor(obj);
        visitNode(obj.getTarget());
        if (deep) {
            visitNode(obj.getDefinition());
        }
        postVisitVisitor(obj);
    }

    @Override
    public void visit(WindowFunction obj) {
        preVisitVisitor(obj);
        visitNode(obj.getFunction());
        visitNode(obj.getWindowSpecification());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(WindowSpecification obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getPartition());
        visitNode(obj.getOrderBy());
        visitNode(obj.getWindowFrame());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(Array array) {
        preVisitVisitor(array);
        visitNodes(array.getExpressions());
        postVisitVisitor(array);
    }

    @Override
    public void visit(ExceptionExpression exceptionExpression) {
        preVisitVisitor(exceptionExpression);
        visitNode(exceptionExpression.getMessage());
        visitNode(exceptionExpression.getSqlState());
        visitNode(exceptionExpression.getErrorCode());
        visitNode(exceptionExpression.getParent());
        postVisitVisitor(exceptionExpression);
    }

    @Override
    public void visit(ReturnStatement obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        postVisitVisitor(obj);
    }

    @Override
    public void visit(IsDistinctCriteria obj) {
        preVisitVisitor(obj);
        //don't visit as that will fail the validation that scalar/row value groupsymbols can't be referenced
        if (!(obj.getLeftRowValue() instanceof GroupSymbol)) {
            visitNode(obj.getLeftRowValue());
        }
        if (!(obj.getRightRowValue() instanceof GroupSymbol)) {
            visitNode(obj.getRightRowValue());
        }
        postVisitVisitor(obj);
    }

    @Override
    public void visit(ExplainCommand explainCommand) {
        preVisitVisitor(explainCommand);
        visitNode(explainCommand.getCommand());
        postVisitVisitor(explainCommand);
    }

    public static void doVisit(LanguageObject object, LanguageVisitor visitor, boolean order) {
        doVisit(object, visitor, order, false);
    }

    public static void doVisit(LanguageObject object, LanguageVisitor visitor, boolean order, boolean deep) {
        PreOrPostOrderNavigator nav = new PreOrPostOrderNavigator(visitor, order, deep);
        object.acceptVisitor(nav);
    }

    public void setSkipEvaluatable(boolean skipEvaluatable) {
        this.skipEvaluatable = skipEvaluatable;
    }

}
