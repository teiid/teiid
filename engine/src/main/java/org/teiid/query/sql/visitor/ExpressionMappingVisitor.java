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

package org.teiid.query.sql.visitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.lang.ObjectTable.ObjectColumn;
import org.teiid.query.sql.lang.XMLTable.XMLColumn;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.proc.AssignmentStatement;
import org.teiid.query.sql.proc.ExceptionExpression;
import org.teiid.query.sql.proc.ReturnStatement;
import org.teiid.query.sql.symbol.*;


/**
 * It is important to use a Post Navigator with this class,
 * otherwise a replacement containing itself will not work
 */
public class ExpressionMappingVisitor extends LanguageVisitor {

    private Map symbolMap;
    private boolean clone = true;
    private boolean elementSymbolsOnly;

    /**
     * Constructor for ExpressionMappingVisitor.
     * @param symbolMap Map of ElementSymbol to Expression
     */
    public ExpressionMappingVisitor(Map symbolMap) {
        this.symbolMap = symbolMap;
    }

    public ExpressionMappingVisitor(Map symbolMap, boolean clone) {
        this.symbolMap = symbolMap;
        this.clone = clone;
    }

    protected boolean createAliases() {
        return true;
    }

    public void visit(Select obj) {
        List<Expression> symbols = obj.getSymbols();
        for (int i = 0; i < symbols.size(); i++) {
            Expression symbol = symbols.get(i);

            if (symbol instanceof MultipleElementSymbol) {
                continue;
            }

            Expression replacmentSymbol = replaceSymbol(symbol, true);

            symbols.set(i, replacmentSymbol);
        }
    }

    public boolean isClone() {
        return clone;
    }

    public void setClone(boolean clone) {
        this.clone = clone;
    }

    @Override
    public void visit(DerivedColumn obj) {
        Expression original = obj.getExpression();
        obj.setExpression(replaceExpression(original));
        if (obj.isPropagateName() && obj.getAlias() == null && original instanceof ElementSymbol) {
            obj.setAlias(((ElementSymbol)original).getShortName());
        }
    }

    @Override
    public void visit(XMLTable obj) {
        for (XMLColumn col : obj.getColumns()) {
            Expression exp = col.getDefaultExpression();
            if (exp != null) {
                col.setDefaultExpression(replaceExpression(exp));
            }
        }
    }

    @Override
    public void visit(JsonTable obj) {
        obj.setJson(replaceExpression(obj.getJson()));
    }

    @Override
    public void visit(ObjectTable obj) {
        for (ObjectColumn col : obj.getColumns()) {
            Expression exp = col.getDefaultExpression();
            if (exp != null) {
                col.setDefaultExpression(replaceExpression(exp));
            }
        }
    }

    @Override
    public void visit(XMLSerialize obj) {
        obj.setExpression(replaceExpression(obj.getExpression()));
    }

    @Override
    public void visit(XMLParse obj) {
        obj.setExpression(replaceExpression(obj.getExpression()));
    }

    private Expression replaceSymbol(Expression ses,
            boolean alias) {
        Expression expr = ses;
        String name = Symbol.getShortName(ses);
        if (ses instanceof ExpressionSymbol) {
            expr = ((ExpressionSymbol)ses).getExpression();
        }

        Expression replacmentSymbol = replaceExpression(expr);

        if (replacmentSymbol == ses) {
            return replacmentSymbol;
        }

        boolean shouldAlias = alias && createAliases() && !Symbol.getShortName(replacmentSymbol).equals(name);

        if (!(replacmentSymbol instanceof Symbol)
                && (!shouldAlias || !(ses instanceof ElementSymbol || ses instanceof AliasSymbol))) {
            replacmentSymbol = new ExpressionSymbol(name, replacmentSymbol);
        } else if (shouldAlias) {
            replacmentSymbol = new AliasSymbol(name, replacmentSymbol);
        }
        return replacmentSymbol;
    }

    /**
     * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.symbol.AliasSymbol)
     */
    public void visit(AliasSymbol obj) {
        Expression replacement = replaceExpression(obj.getSymbol());
        obj.setSymbol(replacement);
    }

    public void visit(ExpressionSymbol expr) {
        expr.setExpression(replaceExpression(expr.getExpression()));
    }

    /**
     * @see org.teiid.query.sql.LanguageVisitor#visit(BetweenCriteria)
     */
    public void visit(BetweenCriteria obj) {
        obj.setExpression( replaceExpression(obj.getExpression()) );
        obj.setLowerExpression( replaceExpression(obj.getLowerExpression()) );
        obj.setUpperExpression( replaceExpression(obj.getUpperExpression()) );
    }

    public void visit(CaseExpression obj) {
        obj.setExpression(replaceExpression(obj.getExpression()));
        final int whenCount = obj.getWhenCount();
        ArrayList whens = new ArrayList(whenCount);
        ArrayList thens = new ArrayList(whenCount);
        for (int i = 0; i < whenCount; i++) {
            whens.add(replaceExpression(obj.getWhenExpression(i)));
            thens.add(replaceExpression(obj.getThenExpression(i)));
        }
        obj.setWhen(whens, thens);
        if (obj.getElseExpression() != null) {
            obj.setElseExpression(replaceExpression(obj.getElseExpression()));
        }
    }

    /**
     * @see org.teiid.query.sql.LanguageVisitor#visit(CompareCriteria)
     */
    public void visit(CompareCriteria obj) {
        obj.setLeftExpression( replaceExpression(obj.getLeftExpression()) );
        obj.setRightExpression( replaceExpression(obj.getRightExpression()) );
    }

    /**
     * @see org.teiid.query.sql.LanguageVisitor#visit(Function)
     */
    public void visit(Function obj) {
        Expression[] args = obj.getArgs();
        if(args != null && args.length > 0) {
            for(int i=0; i<args.length; i++) {
                args[i] = replaceExpression(args[i]);
            }
        }
    }

    /**
     * @see org.teiid.query.sql.LanguageVisitor#visit(IsNullCriteria)
     */
    public void visit(IsNullCriteria obj) {
        obj.setExpression( replaceExpression(obj.getExpression()) );
    }

    /**
     * @see org.teiid.query.sql.LanguageVisitor#visit(MatchCriteria)
     */
    public void visit(MatchCriteria obj) {
        obj.setLeftExpression( replaceExpression(obj.getLeftExpression()) );
        obj.setRightExpression( replaceExpression(obj.getRightExpression()) );
    }

    public void visit(SearchedCaseExpression obj) {
        int whenCount = obj.getWhenCount();
        ArrayList<Expression> thens = new ArrayList<Expression>(whenCount);
        ArrayList<Criteria> whens = new ArrayList<Criteria>(whenCount);
        for (int i = 0; i < whenCount; i++) {
            thens.add(replaceExpression(obj.getThenExpression(i)));
            Expression ex = replaceExpression(obj.getWhenCriteria(i));
            if (!(ex instanceof Criteria)) {
                whens.add(new ExpressionCriteria(ex));
            } else {
                whens.add((Criteria)ex);
            }
        }
        obj.setWhen(whens, thens);
        if (obj.getElseExpression() != null) {
            obj.setElseExpression(replaceExpression(obj.getElseExpression()));
        }
    }

    /**
     * @see org.teiid.query.sql.LanguageVisitor#visit(SetCriteria)
     */
    public void visit(SetCriteria obj) {
        obj.setExpression( replaceExpression(obj.getExpression()) );

        if (obj.isAllConstants()) {
            return;
        }

        Collection newValues = new ArrayList(obj.getValues().size());
        Iterator valueIter = obj.getValues().iterator();
        while(valueIter.hasNext()) {
            newValues.add( replaceExpression( (Expression) valueIter.next() ) );
        }

        obj.setValues(newValues);
    }

    public void visit(DependentSetCriteria obj) {
        obj.setExpression( replaceExpression(obj.getExpression()) );
    }

    /**
     * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.lang.SubqueryCompareCriteria)
     */
    public void visit(SubqueryCompareCriteria obj) {
        obj.setLeftExpression( replaceExpression(obj.getLeftExpression()) );
        if (obj.getArrayExpression() != null) {
            obj.setArrayExpression(replaceExpression(obj.getArrayExpression()));
        }
    }

    /**
     * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.lang.SubquerySetCriteria)
     */
    public void visit(SubquerySetCriteria obj) {
        obj.setExpression( replaceExpression(obj.getExpression()) );
    }

    public Expression replaceExpression(Expression element) {
        if (elementSymbolsOnly && !(element instanceof ElementSymbol)) {
            return element;
        }
        Expression mapped = (Expression) this.symbolMap.get(element);
        if(mapped != null) {
            if (clone) {
                return (Expression)mapped.clone();
            }
            return mapped;
        }
        return element;
    }

    public void visit(StoredProcedure obj) {
        for (Iterator<SPParameter> paramIter = obj.getInputParameters().iterator(); paramIter.hasNext();) {
            SPParameter param = paramIter.next();
            Expression expr = param.getExpression();
            param.setExpression(replaceExpression(expr));
        }
    }

    public void visit(AggregateSymbol obj) {
        visit((Function)obj);
        if (obj.getCondition() != null) {
            obj.setCondition(replaceExpression(obj.getCondition()));
        }
    }

    /**
     * Swap each ElementSymbol in GroupBy (other symbols are ignored).
     * @param obj Object to remap
     */
    public void visit(GroupBy obj) {
        List<Expression> symbols = obj.getSymbols();
        for (int i = 0; i < symbols.size(); i++) {
            Expression symbol = symbols.get(i);
            symbols.set(i, replaceExpression(symbol));
        }
    }

    @Override
    public void visit(OrderByItem obj) {
        obj.setSymbol(replaceSymbol(obj.getSymbol(), obj.getExpressionPosition() != -1));
    }

    public void visit(Limit obj) {
        if (obj.getOffset() != null) {
            obj.setOffset(replaceExpression(obj.getOffset()));
        }
        obj.setRowLimit(replaceExpression(obj.getRowLimit()));
    }

    public void visit(DynamicCommand obj) {
        obj.setSql(replaceExpression(obj.getSql()));
        if (obj.getUsing() != null) {
            for (SetClause clause : obj.getUsing().getClauses()) {
                visit(clause);
            }
        }
    }

    public void visit(SetClause obj) {
        obj.setValue(replaceExpression(obj.getValue()));
    }

    @Override
    public void visit(QueryString obj) {
        obj.setPath(replaceExpression(obj.getPath()));
    }

    @Override
    public void visit(ExpressionCriteria obj) {
        obj.setExpression(replaceExpression(obj.getExpression()));
    }

    /**
     * The object is modified in place, so is not returned.
     * @param obj Language object
     * @param exprMap Expression map, Expression to Expression
     */
    public static void mapExpressions(LanguageObject obj, Map<? extends Expression, ? extends Expression> exprMap) {
        mapExpressions(obj, exprMap, false);
    }

    /**
     * The object is modified in place, so is not returned.
     * @param obj Language object
     * @param exprMap Expression map, Expression to Expression
     */
    public static void mapExpressions(LanguageObject obj, Map<? extends Expression, ? extends Expression> exprMap, boolean deep) {
        if(obj == null || exprMap == null || exprMap.isEmpty()) {
            return;
        }
        final ExpressionMappingVisitor visitor = new ExpressionMappingVisitor(exprMap);
        visitor.elementSymbolsOnly = true;
        boolean preOrder = true;
        boolean useReverseMapping = true;
        for (Map.Entry<? extends Expression, ? extends Expression> entry : exprMap.entrySet()) {
            if (!(entry.getKey() instanceof ElementSymbol)) {
                visitor.elementSymbolsOnly = false;
                break;
            }
        }
        if (!visitor.elementSymbolsOnly) {
            for (Map.Entry<? extends Expression, ? extends Expression> entry : exprMap.entrySet()) {
                if (!(entry.getValue() instanceof ElementSymbol)) {
                    useReverseMapping = !Collections.disjoint(GroupsUsedByElementsVisitor.getGroups(exprMap.keySet()),
                            GroupsUsedByElementsVisitor.getGroups(exprMap.values()));
                    break;
                }
            }
        } else {
            preOrder = false;
            useReverseMapping = false;
        }

        if (useReverseMapping) {
            final Set<Expression> reverseSet = new HashSet<Expression>(exprMap.values());
            PreOrPostOrderNavigator pon = new PreOrPostOrderNavigator(visitor, PreOrPostOrderNavigator.PRE_ORDER, deep) {
                @Override
                protected void visitNode(LanguageObject obj) {
                    if (!(obj instanceof Expression) || !reverseSet.contains(obj)) {
                        super.visitNode(obj);
                    }
                }
            };
            obj.acceptVisitor(pon);
        } else {
            PreOrPostOrderNavigator.doVisit(obj, visitor, preOrder, deep);
        }
    }

    protected void setVariableValues(Map variableValues) {
        this.symbolMap = variableValues;
    }

    protected Map getVariableValues() {
        return symbolMap;
    }

    /**
     * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.proc.AssignmentStatement)
     * @since 5.0
     */
    public void visit(AssignmentStatement obj) {
        obj.setExpression(replaceExpression(obj.getExpression()));
    }

    /**
     * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.lang.Insert)
     * @since 5.0
     */
    public void visit(Insert obj) {
        for (int i = 0; i < obj.getValues().size(); i++) {
            obj.getValues().set(i, replaceExpression((Expression)obj.getValues().get(i)));
        }
    }

    @Override
    public void visit(XMLElement obj) {
        for (int i = 0; i < obj.getContent().size(); i++) {
            obj.getContent().set(i, replaceExpression(obj.getContent().get(i)));
        }
    }

    @Override
    public void visit(WindowSpecification windowSpecification) {
        if (windowSpecification.getPartition() == null) {
            return;
        }
        List<Expression> partition = windowSpecification.getPartition();
        for (int i = 0; i < partition.size(); i++) {
            partition.set(i, replaceExpression(partition.get(i)));
        }
    }

    @Override
    public void visit(Array array) {
        List<Expression> exprs = array.getExpressions();
        for (int i = 0; i < exprs.size(); i++) {
            exprs.set(i, replaceExpression(exprs.get(i)));
        }
    }

    @Override
    public void visit(ExceptionExpression exceptionExpression) {
        if (exceptionExpression.getMessage() != null) {
            exceptionExpression.setMessage(replaceExpression(exceptionExpression.getMessage()));
        }
        if (exceptionExpression.getSqlState() != null) {
            exceptionExpression.setSqlState(replaceExpression(exceptionExpression.getSqlState()));
        }
        if (exceptionExpression.getErrorCode() != null) {
            exceptionExpression.setErrorCode(replaceExpression(exceptionExpression.getErrorCode()));
        }
        if (exceptionExpression.getParent() != null) {
            exceptionExpression.setParent(replaceExpression(exceptionExpression.getParent()));
        }
    }

    @Override
    public void visit(ReturnStatement obj) {
        if (obj.getExpression() != null) {
            obj.setExpression(replaceExpression(obj.getExpression()));
        }
    }

    @Override
    public void visit(IsDistinctCriteria isDistinctCriteria) {
        if (isDistinctCriteria.getLeftRowValue() instanceof Expression) {
            isDistinctCriteria.setLeftRowValue(replaceExpression((Expression)isDistinctCriteria.getLeftRowValue()));
        }
        if (isDistinctCriteria.getRightRowValue() instanceof Expression) {
            isDistinctCriteria.setRightRowValue(replaceExpression((Expression)isDistinctCriteria.getRightRowValue()));
        }
    }

}
