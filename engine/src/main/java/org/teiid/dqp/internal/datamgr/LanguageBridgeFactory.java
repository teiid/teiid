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

package org.teiid.dqp.internal.datamgr;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.RandomAccess;

import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.AggregateFunction;
import org.teiid.language.AndOr;
import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.BatchedUpdates;
import org.teiid.language.BulkCommand;
import org.teiid.language.Call;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.Condition;
import org.teiid.language.DerivedColumn;
import org.teiid.language.DerivedTable;
import org.teiid.language.Exists;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.In;
import org.teiid.language.InsertValueSource;
import org.teiid.language.IsDistinct;
import org.teiid.language.IsNull;
import org.teiid.language.Join;
import org.teiid.language.Like;
import org.teiid.language.Literal;
import org.teiid.language.NamedProcedureCall;
import org.teiid.language.NamedTable;
import org.teiid.language.Not;
import org.teiid.language.Parameter;
import org.teiid.language.QueryExpression;
import org.teiid.language.SearchedCase;
import org.teiid.language.SearchedWhenClause;
import org.teiid.language.Select;
import org.teiid.language.SortSpecification;
import org.teiid.language.SortSpecification.NullOrdering;
import org.teiid.language.SortSpecification.Ordering;
import org.teiid.language.SubqueryComparison;
import org.teiid.language.SubqueryComparison.Quantifier;
import org.teiid.language.SubqueryIn;
import org.teiid.language.TableReference;
import org.teiid.language.WindowSpecification;
import org.teiid.language.With;
import org.teiid.language.WithItem;
import org.teiid.metadata.BaseColumn;
import org.teiid.metadata.Column;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Table;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.function.FunctionMethods;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.optimizer.relational.rules.RulePlaceAccess;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.GroupBy;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.IsDistinctCriteria;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.JoinPredicate;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.MatchCriteria;
import org.teiid.query.sql.lang.NotCriteria;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.SetClause;
import org.teiid.query.sql.lang.SetClauseList;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.lang.SubqueryFromClause;
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.lang.WithQueryCommand;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.AggregateSymbol.Type;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.Array;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.symbol.SearchedCaseExpression;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.symbol.WindowFrame;
import org.teiid.query.sql.symbol.WindowFrame.FrameBound;
import org.teiid.query.sql.symbol.WindowFunction;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.ExecutionFactory.NullOrder;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.TranslatorException;


public class LanguageBridgeFactory {
    private final class TupleBufferList extends AbstractList<List<?>> implements RandomAccess {
        private final TupleBuffer tb;

        private TupleBufferList(TupleBuffer tb) {
            this.tb = tb;
            if (tb.getRowCount() > Integer.MAX_VALUE) {
                throw new AssertionError("TupleBuffer too large for TupleBufferList"); //$NON-NLS-1$
            }
        }

        @Override
        public List<?> get(int index) {
            if (index < 0 || index >= size()) {
                throw new IndexOutOfBoundsException(String.valueOf(index));
            }
            try {
                return tb.getBatch(index+1).getTuple(index+1);
            } catch (TeiidComponentException e) {
                 throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30483, e);
            }
        }

        @Override
        public int size() {
            return (int)tb.getRowCount();
        }
    }

    private final class TupleSourceIterator implements Iterator<List<?>> {
        private final TupleSource ts;
        List<?> nextRow;

        private TupleSourceIterator(TupleSource ts) {
            this.ts = ts;
        }

        @Override
        public boolean hasNext() {
            if (nextRow == null) {
                try {
                    nextRow = ts.nextTuple();
                } catch (TeiidComponentException e) {
                     throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30484, e);
                } catch (TeiidProcessingException e) {
                     throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30485, e);
                }
            }
            return nextRow != null;
        }

        @Override
        public List<?> next() {
            if (nextRow == null && !hasNext()) {
                throw new NoSuchElementException();
            }
            List<?> result = nextRow;
            nextRow = null;
            return result;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private RuntimeMetadataImpl metadataFactory = null;
    private int valueIndex = 0;
    private List<List<?>> allValues = new LinkedList<List<?>>();
    private Map<String, List<? extends List<?>>> dependentSets;
    private boolean convertIn;
    private boolean supportsConcat2;
    private int maxInCriteriaSize;
    private boolean supportsCountBig;

    //state to handle with name exclusion
    private IdentityHashMap<Object, GroupSymbol> remappedGroups;
    private String excludeWithName;
    private CommandContext commandContext;

    //state to handle null ordering
    private boolean supportsNullOrdering;
    private NullOrder sourceNullOrder;

    private boolean readOnly = true;

    public LanguageBridgeFactory(QueryMetadataInterface metadata) {
        if (metadata != null) {
            metadataFactory = new RuntimeMetadataImpl(metadata);
        }
    }

    public LanguageBridgeFactory(RuntimeMetadataImpl metadata) {
        this.metadataFactory = metadata;
    }

    public void setConvertIn(boolean convertIn) {
        this.convertIn = convertIn;
    }

    public void setSupportsConcat2(boolean supportsConcat2) {
        this.supportsConcat2 = supportsConcat2;
    }

    public void setExcludeWithName(String excludeWithName) {
        this.excludeWithName = excludeWithName;
    }

    public void setSupportsCountBig(boolean supportsCountBig) {
        this.supportsCountBig = supportsCountBig;
    }

    public org.teiid.language.Command translate(Command command) {
        try {
            if (command == null) {
                return null;
            }
            if (command instanceof Query) {
                Select result = translate((Query)command);
                result.setDependentValues(this.dependentSets);
                setProjected(result);
                return result;
            } else if (command instanceof SetQuery) {
                org.teiid.language.SetQuery result = translate((SetQuery)command);
                setProjected(result);
                return result;
            } else if (command instanceof Insert) {
                readOnly = false;
                return translate((Insert)command);
            } else if (command instanceof Update) {
                readOnly = false;
                return translate((Update)command);
            } else if (command instanceof Delete) {
                readOnly = false;
                return translate((Delete)command);
            } else if (command instanceof StoredProcedure) {
                return translate((StoredProcedure)command);
            } else if (command instanceof BatchedUpdateCommand) {
                readOnly = false;
                return translate((BatchedUpdateCommand)command);
            }
            throw new AssertionError(command.getClass().getName() + " " + command); //$NON-NLS-1$
        } finally {
            this.allValues.clear();
            this.dependentSets = null;
            this.valueIndex = 0;
        }
    }

    private void setProjected(QueryExpression qe) {
        if (qe instanceof Select) {
            Select select = (Select)qe;
            for (DerivedColumn dc : select.getDerivedColumns()) {
                dc.setProjected(true);
            }
        } else {
            org.teiid.language.SetQuery sq = (org.teiid.language.SetQuery)qe;
            setProjected(sq.getLeftQuery());
            setProjected(sq.getRightQuery());
        }
    }

    QueryExpression translate(QueryCommand command) {
        if (command instanceof Query) {
            return translate((Query)command);
        }
        return translate((SetQuery)command);
    }

    org.teiid.language.SetQuery translate(SetQuery union) {
        org.teiid.language.SetQuery result = new org.teiid.language.SetQuery();
        result.setWith(translate(union.getWith()));
        result.setAll(union.isAll());
        switch (union.getOperation()) {
            case UNION:
                result.setOperation(org.teiid.language.SetQuery.Operation.UNION);
                break;
            case INTERSECT:
                result.setOperation(org.teiid.language.SetQuery.Operation.INTERSECT);
                break;
            case EXCEPT:
                result.setOperation(org.teiid.language.SetQuery.Operation.EXCEPT);
                break;
        }
        result.setLeftQuery(translate(union.getLeftQuery()));
        result.setRightQuery(translate(union.getRightQuery()));
        result.setOrderBy(translate(union.getOrderBy(), true));
        result.setLimit(translate(union.getLimit()));
        return result;
    }

    /* Query */
    Select translate(Query query) {
        With with = translate(query.getWith());
        List<Expression> symbols = query.getSelect().getSymbols();
        List<DerivedColumn> translatedSymbols = new ArrayList<DerivedColumn>(symbols.size());
        for (Iterator<Expression> i = symbols.iterator(); i.hasNext();) {
            Expression symbol = i.next();
            String alias = null;
            if(symbol instanceof AliasSymbol) {
                alias = ((AliasSymbol)symbol).getOutputName();
                symbol = ((AliasSymbol)symbol).getSymbol();
            }

            org.teiid.language.Expression iExp = translate(symbol);

            DerivedColumn selectSymbol = new DerivedColumn(alias, iExp);
            translatedSymbols.add(selectSymbol);
        }
        List<TableReference> items = null;
        if (query.getFrom() != null) {
            List<FromClause> clauses = query.getFrom().getClauses();
            items = new ArrayList<TableReference>(clauses.size());
            for (Iterator<FromClause> i = clauses.iterator(); i.hasNext();) {
                items.add(translate(i.next()));
            }
        }
        Select q = new Select(translatedSymbols, query
                .getSelect().isDistinct(), items,
                translate(query.getCriteria()), translate(query.getGroupBy()),
                translate(query.getHaving()), translate(query.getOrderBy(), false));
        q.setLimit(translate(query.getLimit()));
        q.setWith(with);
        return q;
    }

    public With translate(List<WithQueryCommand> with) {
        if (with == null || with.isEmpty()) {
            return null;
        }
        With result = new With();
        ArrayList<WithItem> items = new ArrayList<WithItem>(with.size());
        for (WithQueryCommand withQueryCommand : with) {
            WithItem item = new WithItem();
            GroupSymbol group = withQueryCommand.getGroupSymbol();
            if (withQueryCommand.getCommand() != null && excludeWithName != null && excludeWithName.equalsIgnoreCase(group.getName())) {
                group = RulePlaceAccess.recontextSymbol(withQueryCommand.getGroupSymbol(), commandContext.getGroups());
                group.setDefinition(null);
                if (remappedGroups == null) {
                    remappedGroups = new IdentityHashMap<Object, GroupSymbol>();
                }
                this.remappedGroups.put(group.getMetadataID(), group);
            }
            item.setTable(translate(group));
            if (withQueryCommand.getColumns() != null) {
                List<ColumnReference> translatedElements = new ArrayList<ColumnReference>(withQueryCommand.getColumns().size());
                for (ElementSymbol es: withQueryCommand.getColumns()) {
                    ColumnReference cr = translate(es);
                    translatedElements.add(cr);
                    if (withQueryCommand.getCommand() == null) {
                        //we want to convey the metadata to the source layer if possible
                        Object mid = es.getMetadataID();
                        if (mid instanceof TempMetadataID) {
                            TempMetadataID tid = (TempMetadataID)mid;
                            mid = tid.getOriginalMetadataID();
                        }
                        if (mid instanceof Column) {
                            cr.setMetadataObject((Column)mid);
                        }
                    }
                }
                item.setColumns(translatedElements);
            }
            if (withQueryCommand.getCommand() != null) {
                item.setSubquery(translate(withQueryCommand.getCommand()));
            } else {
                item.setDependentValues(new TupleBufferList(withQueryCommand.getTupleBuffer()));
            }
            item.setRecusive(withQueryCommand.isRecursive());
            items.add(item);
        }
        result.setItems(items);
        return result;
    }

    public TableReference translate(FromClause clause) {
        if (clause == null) {
            return null;
        }
        if (clause instanceof JoinPredicate) {
            return translate((JoinPredicate)clause);
        } else if (clause instanceof SubqueryFromClause) {
            return translate((SubqueryFromClause)clause);
        } else if (clause instanceof UnaryFromClause) {
            return translate((UnaryFromClause)clause);
        }
        throw new AssertionError(clause.getClass().getName() + " " + clause); //$NON-NLS-1$
    }

    Join translate(JoinPredicate join) {
        List crits = join.getJoinCriteria();
        Criteria crit = null;
        if (crits.size() == 1) {
            crit = (Criteria)crits.get(0);
        } else if (crits.size() > 1) {
            crit = new CompoundCriteria(crits);
        }

        Join.JoinType joinType = Join.JoinType.INNER_JOIN;
        if(join.getJoinType().equals(JoinType.JOIN_INNER)) {
            joinType = Join.JoinType.INNER_JOIN;
        } else if(join.getJoinType().equals(JoinType.JOIN_LEFT_OUTER)) {
            joinType = Join.JoinType.LEFT_OUTER_JOIN;
        } else if(join.getJoinType().equals(JoinType.JOIN_RIGHT_OUTER)) {
            joinType = Join.JoinType.RIGHT_OUTER_JOIN;
        } else if(join.getJoinType().equals(JoinType.JOIN_FULL_OUTER)) {
            joinType = Join.JoinType.FULL_OUTER_JOIN;
        } else if(join.getJoinType().equals(JoinType.JOIN_CROSS)) {
            joinType = Join.JoinType.CROSS_JOIN;
        }

        return new Join(translate(join.getLeftClause()),
                            translate(join.getRightClause()),
                            joinType,
                            translate(crit));
    }

    TableReference translate(SubqueryFromClause clause) {
        if (clause.getCommand() instanceof StoredProcedure) {
            NamedProcedureCall result = new NamedProcedureCall(translate((StoredProcedure)clause.getCommand()), clause.getOutputName());
            result.setLateral(clause.isLateral());
            result.getCall().setTableReference(true);
            return result;
        }
        DerivedTable result = new DerivedTable(translate((QueryCommand)clause.getCommand()), clause.getOutputName());
        result.setLateral(clause.isLateral());
        return result;
    }

    NamedTable translate(UnaryFromClause clause) {
        return translate(clause.getGroup());
    }

    public Condition translate(Criteria criteria) {
        if (criteria == null) {
            return null;
        }
        if (criteria instanceof CompareCriteria) {
            return translate((CompareCriteria)criteria);
        } else if (criteria instanceof CompoundCriteria) {
            return translate((CompoundCriteria)criteria);
        } else if (criteria instanceof ExistsCriteria) {
            return translate((ExistsCriteria)criteria);
        } else if (criteria instanceof IsNullCriteria) {
            return translate((IsNullCriteria)criteria);
        }else if (criteria instanceof MatchCriteria) {
            return translate((MatchCriteria)criteria);
        } else if (criteria instanceof NotCriteria) {
            return translate((NotCriteria)criteria);
        } else if (criteria instanceof SetCriteria) {
            return translate((SetCriteria)criteria);
        } else if (criteria instanceof SubqueryCompareCriteria) {
            return translate((SubqueryCompareCriteria)criteria);
        } else if (criteria instanceof SubquerySetCriteria) {
            return translate((SubquerySetCriteria)criteria);
        } else if (criteria instanceof DependentSetCriteria) {
            return translate((DependentSetCriteria)criteria);
        } else if (criteria instanceof IsDistinctCriteria) {
            return translate((IsDistinctCriteria)criteria);
        }
        throw new AssertionError(criteria.getClass().getName() + " " + criteria); //$NON-NLS-1$
    }

    org.teiid.language.IsDistinct translate(IsDistinctCriteria criteria) {
        return new IsDistinct(translate((Expression) criteria.getLeftRowValue()),
                translate((Expression)criteria.getRightRowValue()), criteria.isNegated());
    }

    org.teiid.language.Comparison translate(DependentSetCriteria criteria) {
        Operator operator = Operator.EQ;
        org.teiid.language.Expression arg = null;
        final TupleBuffer tb = criteria.getDependentValueSource().getTupleBuffer();
        if (criteria.getValueExpression() instanceof Array) {
            Array array = (Array)criteria.getValueExpression();
            List<org.teiid.language.Expression> params = new ArrayList<org.teiid.language.Expression>();
            Class<?> baseType = null;
            for (Expression ex : array.getExpressions()) {
                if (baseType == null) {
                    baseType = ex.getType();
                } else if (!baseType.equals(ex.getType())) {
                    baseType = DataTypeManager.DefaultDataClasses.OBJECT;
                }
                params.add(createParameter(criteria, tb, ex));
            }
            arg = new org.teiid.language.Array(baseType, params);
        } else {
            Expression ex = criteria.getValueExpression();
            arg = createParameter(criteria, tb, ex);
        }
        if (this.dependentSets == null) {
            this.dependentSets = new HashMap<String, List<? extends List<?>>>();
        }
        this.dependentSets.put(criteria.getContextSymbol(), new TupleBufferList(tb));
        Comparison result = new org.teiid.language.Comparison(translate(criteria.getExpression()),
                                        arg, operator);
        return result;
    }

    private Parameter createParameter(DependentSetCriteria criteria,
            final TupleBuffer tb, Expression ex) {
        Parameter p = new Parameter();
        p.setType(ex.getType());
        p.setValueIndex(tb.getSchema().indexOf(ex));
        p.setDependentValueId(criteria.getContextSymbol());
        return p;
    }

    org.teiid.language.Comparison translate(CompareCriteria criteria) {
        Operator operator = Operator.EQ;
        switch(criteria.getOperator()) {
            case CompareCriteria.EQ:
                operator = Operator.EQ;
                break;
            case CompareCriteria.NE:
                operator = Operator.NE;
                break;
            case CompareCriteria.LT:
                operator = Operator.LT;
                break;
            case CompareCriteria.LE:
                operator = Operator.LE;
                break;
            case CompareCriteria.GT:
                operator = Operator.GT;
                break;
            case CompareCriteria.GE:
                operator = Operator.GE;
                break;

        }

        return new org.teiid.language.Comparison(translate(criteria.getLeftExpression()),
                                        translate(criteria.getRightExpression()), operator);
    }

    AndOr translate(CompoundCriteria criteria) {
        List nestedCriteria = criteria.getCriteria();
        int size = nestedCriteria.size();
        AndOr.Operator op = criteria.getOperator() == CompoundCriteria.AND?AndOr.Operator.AND:AndOr.Operator.OR;
        AndOr result = new AndOr(translate((Criteria)nestedCriteria.get(size - 2)), translate((Criteria)nestedCriteria.get(size - 1)), op);
        for (int i = nestedCriteria.size() - 3; i >= 0; i--) {
            result = new AndOr(translate((Criteria)nestedCriteria.get(i)), result, op);
        }
        return result;
    }

    Condition translate(ExistsCriteria criteria) {
        Exists exists = new Exists(translate(criteria.getCommand()));
        if (criteria.isNegated()) {
            return new Not(exists);
        }
        return exists;
    }

    IsNull translate(IsNullCriteria criteria) {
        return new IsNull(translate(criteria.getExpression()), criteria.isNegated());
    }

    Like translate(MatchCriteria criteria) {
        Character escapeChar = null;
        if(criteria.getEscapeChar() != MatchCriteria.NULL_ESCAPE_CHAR) {
            escapeChar = new Character(criteria.getEscapeChar());
        }
        Like like = new Like(translate(criteria.getLeftExpression()),
                                    translate(criteria.getRightExpression()),
                                    escapeChar,
                                    criteria.isNegated());
        like.setMode(criteria.getMode());
        return like;
    }

    Condition translate(SetCriteria criteria) {
        Collection expressions = criteria.getValues();
        List<org.teiid.language.Expression> translatedExpressions = translateExpressionList(expressions);
        org.teiid.language.Expression expr = translate(criteria.getExpression());
        if (convertIn) {
            Condition condition = null;
            for (org.teiid.language.Expression expression : translatedExpressions) {
                if (condition == null) {
                    condition = new Comparison(expr, expression, criteria.isNegated()?Operator.NE:Operator.EQ);
                } else {
                    condition = new AndOr(new Comparison(expr, expression, criteria.isNegated()?Operator.NE:Operator.EQ), condition, criteria.isNegated()?AndOr.Operator.AND:AndOr.Operator.OR);
                }
            }
            return condition;
        }
        if (maxInCriteriaSize > 0 && translatedExpressions.size() > maxInCriteriaSize) {
            Condition condition = null;
            int count = translatedExpressions.size()/maxInCriteriaSize + ((translatedExpressions.size()%maxInCriteriaSize!=0)?1:0);
            for (int i = 0; i < count; i++) {
                List<org.teiid.language.Expression> subList = translatedExpressions.subList(maxInCriteriaSize*i, Math.min(translatedExpressions.size(), maxInCriteriaSize*(i+1)));
                List<org.teiid.language.Expression> translatedExpressionsSubList = new ArrayList<org.teiid.language.Expression>(subList);
                if (condition == null) {
                    condition = new In(expr, translatedExpressionsSubList, criteria.isNegated());
                } else {
                    condition = new AndOr(condition, new In(expr, translatedExpressionsSubList, criteria.isNegated()), criteria.isNegated()?AndOr.Operator.AND:AndOr.Operator.OR);
                }
            }
            return condition;
        }
        return new In(expr,
                                  translatedExpressions,
                                  criteria.isNegated());
    }

    SubqueryComparison translate(SubqueryCompareCriteria criteria) {
        Quantifier quantifier = Quantifier.ALL;
        switch(criteria.getPredicateQuantifier()) {
            case SubqueryCompareCriteria.ALL:
                quantifier = Quantifier.ALL;
                break;
            case SubqueryCompareCriteria.ANY:
                quantifier = Quantifier.SOME;
                break;
            case SubqueryCompareCriteria.SOME:
                quantifier = Quantifier.SOME;
                break;
        }

        Operator operator = Operator.EQ;
        switch(criteria.getOperator()) {
            case SubqueryCompareCriteria.EQ:
                operator = Operator.EQ;
                break;
            case SubqueryCompareCriteria.NE:
                operator = Operator.NE;
                break;
            case SubqueryCompareCriteria.LT:
                operator = Operator.LT;
                break;
            case SubqueryCompareCriteria.LE:
                operator = Operator.LE;
                break;
            case SubqueryCompareCriteria.GT:
                operator = Operator.GT;
                break;
            case SubqueryCompareCriteria.GE:
                operator = Operator.GE;
                break;
        }

        return new SubqueryComparison(translate(criteria.getLeftExpression()),
                                  operator,
                                  quantifier,
                                  translate(criteria.getCommand()));
    }

    SubqueryIn translate(SubquerySetCriteria criteria) {
        return new SubqueryIn(translate(criteria.getExpression()),
                                  criteria.isNegated(),
                                  translate(criteria.getCommand()));
    }

    Not translate(NotCriteria criteria) {
        return new Not(translate(criteria.getCriteria()));
    }

    public org.teiid.language.GroupBy translate(GroupBy groupBy) {
        if(groupBy == null){
            return null;
        }
        List items = groupBy.getSymbols();
        List<org.teiid.language.Expression> translatedItems = new ArrayList<org.teiid.language.Expression>();
        for (Iterator i = items.iterator(); i.hasNext();) {
            translatedItems.add(translate((Expression)i.next()));
        }
        org.teiid.language.GroupBy result = new org.teiid.language.GroupBy(translatedItems);
        result.setRollup(groupBy.isRollup());
        return result;
    }

    public org.teiid.language.OrderBy translate(OrderBy orderBy, boolean set) {
        if(orderBy == null){
            return null;
        }
        List<OrderByItem> items = orderBy.getOrderByItems();
        List<SortSpecification> translatedItems = new ArrayList<SortSpecification>();
        for (int i = 0; i < items.size(); i++) {
            Expression symbol = items.get(i).getSymbol();
            Ordering direction = items.get(i).isAscending() ? Ordering.ASC: Ordering.DESC;

            SortSpecification orderByItem = null;
            if(!set && (items.get(i).isUnrelated() || symbol instanceof ElementSymbol)){
                orderByItem = new SortSpecification(direction, translate(symbol));
            } else {
                orderByItem = new SortSpecification(direction, new ColumnReference(null, Symbol.getShortName(((Symbol)symbol).getOutputName()), null, symbol.getType()));
            }
            orderByItem.setNullOrdering(items.get(i).getNullOrdering());
            translatedItems.add(orderByItem);
        }
        org.teiid.language.OrderBy result = new org.teiid.language.OrderBy(translatedItems);

        if (orderBy.isUserOrdering() && commandContext != null) {
            NullOrder teiidNullOrder = commandContext.getOptions().getDefaultNullOrder();
            if (!supportsNullOrdering
                    || (sourceNullOrder != teiidNullOrder && commandContext.getOptions().isPushdownDefaultNullOrder())) {
                correctNullOrdering(result,
                    supportsNullOrdering, sourceNullOrder, commandContext.getOptions().getDefaultNullOrder());
            }
        }

        return result;
    }


    /* Expressions */
    public org.teiid.language.Expression translate(Expression expr) {
        if (expr == null) {
            return null;
        }
        if (expr instanceof Constant) {
            return translate((Constant)expr);
        } else if (expr instanceof AggregateSymbol) {
            return translate((AggregateSymbol)expr);
        } else if (expr instanceof Function) {
            return translate((Function)expr);
        } else if (expr instanceof ScalarSubquery) {
            return translate((ScalarSubquery)expr);
        } else if (expr instanceof SearchedCaseExpression) {
            return translate((SearchedCaseExpression)expr);
        } else if (expr instanceof ElementSymbol) {
            return translate((ElementSymbol)expr);
        } else if (expr instanceof ExpressionSymbol) {
            return translate((ExpressionSymbol)expr);
        } else if (expr instanceof Criteria) {
            Condition c = translate((Criteria)expr);
            c.setExpression(true);
            return c;
        } else if (expr instanceof WindowFunction) {
            return translate((WindowFunction)expr);
        } else if (expr instanceof Array) {
            return translate((Array)expr);
        }
        throw new AssertionError(expr.getClass().getName() + " " + expr); //$NON-NLS-1$
    }

    org.teiid.language.Array translate(Array array) {
        return new org.teiid.language.Array(array.getComponentType(), translateExpressionList(array.getExpressions()));
    }

    org.teiid.language.WindowFunction translate(WindowFunction windowFunction) {
        org.teiid.language.WindowFunction result = new org.teiid.language.WindowFunction();
        result.setFunction(translate(windowFunction.getFunction()));
        WindowSpecification ws = new WindowSpecification();
        ws.setOrderBy(translate(windowFunction.getWindowSpecification().getOrderBy(), false));
        List<Expression> partition = windowFunction.getWindowSpecification().getPartition();
        if (partition != null) {
            ArrayList<org.teiid.language.Expression> partitionList = translateExpressionList(partition);
            ws.setPartition(partitionList);
        }
        WindowFrame frame = windowFunction.getWindowSpecification().getWindowFrame();
        if (frame != null) {
            org.teiid.language.WindowFrame resultFrame = new org.teiid.language.WindowFrame(frame.getMode());
            resultFrame.setStart(translate(frame.getStart()));
            resultFrame.setEnd(translate(frame.getEnd()));
            ws.setWindowFrame(resultFrame);
        }
        result.setWindowSpecification(ws);
        return result;
    }

    org.teiid.language.WindowFrame.FrameBound translate(FrameBound frameBound) {
        if (frameBound == null) {
            return null;
        }
        org.teiid.language.WindowFrame.FrameBound result = new org.teiid.language.WindowFrame.FrameBound(frameBound.getBoundMode());
        result.setBound(frameBound.getBound());
        return result;
    }

    private ArrayList<org.teiid.language.Expression> translateExpressionList(
            Collection<? extends Expression> list) {
        ArrayList<org.teiid.language.Expression> result = new ArrayList<org.teiid.language.Expression>(list.size());
        for (Expression ex : list) {
            result.add(translate(ex));
        }
        return result;
    }

    org.teiid.language.Expression translate(Constant constant) {
        if (constant.isMultiValued()) {
            Parameter result = new Parameter();
            result.setType(constant.getType());
            final List<?> values = (List<?>)constant.getValue();
            allValues.add(values);
            result.setValueIndex(valueIndex++);
            return result;
        }
        if (constant.getValue() instanceof ArrayImpl) {
            //TODO: we could check if there is a common base type (also needs to be in the dependent logic)
            // and expand binding options in the translators

            //we currently support the notion of a mixed type array, since we consider object a common base type
            //that will not work for all sources, so instead of treating this as a single array (as commented out below),
            //we just turn it into an array of parameters
            //Literal result = new Literal(av.getValues(), org.teiid.language.Array.class);
            //result.setBindEligible(constant.isBindEligible());
            //return result;

            ArrayImpl av = (ArrayImpl)constant.getValue();
            List<Constant> vals = new ArrayList<Constant>();
            Class<?> baseType = null;
            for (Object o : av.getValues()) {
                Constant c = new Constant(o);
                c.setBindEligible(constant.isBindEligible());
                vals.add(c);
                if (baseType == null) {
                    baseType = c.getType();
                } else if (!baseType.equals(c.getType())) {
                    if (baseType == DataTypeManager.DefaultDataClasses.NULL) {
                        baseType = c.getType();
                    } else if (c.getType() != DataTypeManager.DefaultDataClasses.NULL) {
                        baseType = DataTypeManager.DefaultDataClasses.OBJECT;
                    }
                }
            }
            return new org.teiid.language.Array(baseType, translateExpressionList(vals));
        }
        Literal result = new Literal(constant.getValue(), constant.getType());
        result.setBindEligible(constant.isBindEligible());
        return result;
    }

    org.teiid.language.Expression translate(Function function) {
        Expression [] args = function.getArgs();
        List<org.teiid.language.Expression> params = new ArrayList<org.teiid.language.Expression>(args.length);
        for (int i = 0; i < args.length; i++) {
            params.add(translate(args[i]));
        }
        String name = function.getName();
        FunctionMethod method = null;
        FunctionDescriptor functionDescriptor = function.getFunctionDescriptor();
        if (functionDescriptor != null) {
            method = functionDescriptor.getMethod();
            name = functionDescriptor.getName();
            if (!supportsConcat2 && method.getParent() == null && name.equalsIgnoreCase(SourceSystemFunctions.CONCAT2)) {
                Expression[] newArgs = new Expression[args.length];

                boolean useCase = true;
                for(int i=0; i<args.length; i++) {
                    if (args[i] instanceof Constant) {
                        newArgs[i] = args[i];
                        useCase = false;
                    } else {
                        Function f = new Function(SourceSystemFunctions.IFNULL, new Expression[] {args[i], new Constant("")}); //$NON-NLS-1$
                        newArgs[i] = f;
                        f.setType(args[i].getType());
                        FunctionDescriptor descriptor =
                            metadataFactory.getMetadata().getFunctionLibrary().findFunction(SourceSystemFunctions.IFNULL, new Class[] { args[i].getType(), DataTypeManager.DefaultDataClasses.STRING });
                        f.setFunctionDescriptor(descriptor);
                    }
                }

                Function concat = new Function(SourceSystemFunctions.CONCAT, newArgs);
                concat.setType(DataTypeManager.DefaultDataClasses.STRING);

                if (!useCase) {
                    return translate(concat);
                }

                FunctionDescriptor descriptor =
                    metadataFactory.getMetadata().getFunctionLibrary().findFunction(SourceSystemFunctions.CONCAT, new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.STRING });
                concat.setFunctionDescriptor(descriptor);

                List<CompoundCriteria> when = Arrays.asList(new CompoundCriteria(CompoundCriteria.AND, new IsNullCriteria(args[0]), new IsNullCriteria(args[1])));
                Constant nullConstant = new Constant(null, DataTypeManager.DefaultDataClasses.STRING);
                List<Constant> then = Arrays.asList(nullConstant);
                SearchedCaseExpression caseExpr = new SearchedCaseExpression(when, then);
                caseExpr.setElseExpression(concat);
                caseExpr.setType(DataTypeManager.DefaultDataClasses.STRING);
                return translate(caseExpr);
            }
            if (method.getParent() == null && name.equalsIgnoreCase(SourceSystemFunctions.TIMESTAMPADD)
                    && function.getArg(1).getType() == DataTypeManager.DefaultDataClasses.LONG) {
                //TEIID-5406 only allow integer literal pushdown for backwards compatibility
                if (params.get(1) instanceof Literal) {
                    try {
                        params.set(1, new Literal(FunctionMethods.integerRangeCheck((Long)((Literal)params.get(1)).getValue()), DataTypeManager.DefaultDataClasses.INTEGER));
                    } catch (FunctionExecutionException e) {
                        //corner case - for now we'll just throw an exception, but we could also prevent pushdown
                        throw new TeiidRuntimeException(QueryPlugin.Event.TEIID31275, e);
                    }
                } else {
                    //cast - will be supported by the check in CriteriaCapabilityValidatorVisitor
                    params.set(1, new org.teiid.language.Function(SourceSystemFunctions.CONVERT,
                            Arrays.asList(params.get(1), new Literal(DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataClasses.STRING)), DataTypeManager.DefaultDataClasses.INTEGER));
                }
            }

            if (function.getPushdownFunction() != null) {
                method = function.getPushdownFunction();
                name = method.getName();
            }

            //check for translator pushdown functions, and use the name in source if possible
            if (method.getNameInSource() != null &&
                    (CoreConstants.SYSTEM_MODEL.equals(functionDescriptor.getSchema())
                            || (method.getParent() != null && method.getParent().isPhysical()))) {
                name = method.getNameInSource();
            }
        } else {
            name = Symbol.getShortName(name);
        }

        //if there is any ambiguity in the function name it will be up to the translator logic to check the
        //metadata
        org.teiid.language.Function result = new org.teiid.language.Function(name, params, function.getType());
        result.setMetadataObject(method);
        return result;
    }

    SearchedCase translate(SearchedCaseExpression expr) {
        ArrayList<SearchedWhenClause> whens = new ArrayList<SearchedWhenClause>();
        for (int i = 0; i < expr.getWhenCount(); i++) {
            whens.add(new SearchedWhenClause(translate(expr.getWhenCriteria(i)), translate(expr.getThenExpression(i))));
        }
        return new SearchedCase(whens,
                                      translate(expr.getElseExpression()),
                                      expr.getType());
    }


    org.teiid.language.Expression translate(ScalarSubquery ss) {
        return new org.teiid.language.ScalarSubquery(translate(ss.getCommand()));
    }

    org.teiid.language.Expression translate(AliasSymbol symbol) {
        return translate(symbol.getSymbol());
    }

    ColumnReference translate(ElementSymbol symbol) {
        ColumnReference element = new ColumnReference(translate(symbol.getGroupSymbol()), Symbol.getShortName(symbol.getOutputName()), null, symbol.getType());
        if (element.getTable().getMetadataObject() == null) {
            //handle procedure resultset columns
            if (symbol.getMetadataID() instanceof TempMetadataID) {
                TempMetadataID tid = (TempMetadataID)symbol.getMetadataID();
                if (tid.getOriginalMetadataID() instanceof Column && !(((Column)tid.getOriginalMetadataID()).getParent() instanceof Table)) {
                    element.setMetadataObject(metadataFactory.getElement(tid.getOriginalMetadataID()));
                }
            }
            return element;
        }

        Object mid = symbol.getMetadataID();

        element.setMetadataObject(metadataFactory.getElement(mid));
        return element;
    }

    AggregateFunction translate(AggregateSymbol symbol) {
        List<org.teiid.language.Expression> params = new ArrayList<org.teiid.language.Expression>(symbol.getArgs().length);
        for (Expression expression : symbol.getArgs()) {
            params.add(translate(expression));
        }
        String name = symbol.getAggregateFunction().name();
        if (symbol.getFunctionDescriptor() != null) {
            name = Symbol.getShortName(symbol.getFunctionDescriptor().getName());
        } else if (symbol.getAggregateFunction() == AggregateSymbol.Type.USER_DEFINED) {
            name = symbol.getName();
        } else if (symbol.getAggregateFunction() == Type.COUNT_BIG && !supportsCountBig) {
            name = Type.COUNT.name();
        }

        AggregateFunction af = new AggregateFunction(name,
                                symbol.isDistinct(),
                                params,
                                symbol.getType());
        af.setCondition(translate(symbol.getCondition()));
        af.setOrderBy(translate(symbol.getOrderBy(), false));
        if (symbol.getFunctionDescriptor() != null) {
            af.setMetadataObject(symbol.getFunctionDescriptor().getMethod());
        }
        return af;
    }

    org.teiid.language.Expression translate(ExpressionSymbol symbol) {
        return translate(symbol.getExpression());
    }

    /* Insert */
    org.teiid.language.Insert translate(Insert insert) {
        List<ElementSymbol> elements = insert.getVariables();
        List<ColumnReference> translatedElements = new ArrayList<ColumnReference>();

        for (ElementSymbol elementSymbol : elements) {
            translatedElements.add(translate(elementSymbol));
        }
        Iterator<List<?>> parameterValues = null;
        InsertValueSource valueSource = null;
        if (insert.getQueryExpression() != null) {
            valueSource = translate(insert.getQueryExpression());
        } else if (insert.getTupleSource() != null) {
            final TupleSource ts = insert.getTupleSource();
            parameterValues = new TupleSourceIterator(ts);
            List<org.teiid.language.Expression> translatedValues = new ArrayList<org.teiid.language.Expression>();
            for (int i = 0; i < insert.getVariables().size(); i++) {
                ElementSymbol es = insert.getVariables().get(i);
                Parameter param = new Parameter();
                param.setType(es.getType());
                param.setValueIndex(i);
                translatedValues.add(param);
            }
            valueSource = new ExpressionValueSource(translatedValues);
        } else {
            // This is for the simple one row insert.
            List values = insert.getValues();
            List<org.teiid.language.Expression> translatedValues = new ArrayList<org.teiid.language.Expression>();
            for (Iterator i = values.iterator(); i.hasNext();) {
                translatedValues.add(translate((Expression)i.next()));
            }
            valueSource = new ExpressionValueSource(translatedValues);
        }

        org.teiid.language.Insert result = new org.teiid.language.Insert(translate(insert.getGroup()),
                              translatedElements,
                              valueSource);
        result.setParameterValues(parameterValues);
        setBatchValues(result);
        result.setUpsert(insert.isUpsert());
        return result;
    }

    private void setBatchValues(BulkCommand bc) {
        if (valueIndex == 0) {
            return;
        }
        if (bc.getParameterValues() != null) {
            throw new IllegalStateException("Already set batch values"); //$NON-NLS-1$
        }
        int rowCount = allValues.get(0).size();
        List<List<?>> result = new ArrayList<List<?>>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            List<Object> row = new ArrayList<Object>(allValues.size());
            for (List<?> vals : allValues) {
                row.add(vals.get(i));
            }
            result.add(row);
        }
        bc.setParameterValues(result.iterator());
    }

    /* Update */
    org.teiid.language.Update translate(Update update) {
        org.teiid.language.Update result = new org.teiid.language.Update(translate(update.getGroup()),
                              translate(update.getChangeList()),
                              translate(update.getCriteria()));
        setBatchValues(result);
        return result;
    }

    List<org.teiid.language.SetClause> translate(SetClauseList setClauseList) {
        List<org.teiid.language.SetClause> clauses = new ArrayList<org.teiid.language.SetClause>(setClauseList.getClauses().size());
        for (SetClause setClause : setClauseList.getClauses()) {
            clauses.add(translate(setClause));
        }
        return clauses;
    }

    org.teiid.language.SetClause translate(SetClause setClause) {
        return new org.teiid.language.SetClause(translate(setClause.getSymbol()), translate(setClause.getValue()));
    }

    /* Delete */
    org.teiid.language.Delete translate(Delete delete) {
        org.teiid.language.Delete deleteImpl = new org.teiid.language.Delete(translate(delete.getGroup()),
                              translate(delete.getCriteria()));
        setBatchValues(deleteImpl);
        return deleteImpl;
    }

    /* Execute */
    Call translate(StoredProcedure sp) {
        Procedure proc = null;
        if(sp.getProcedureID() != null) {
            try {
                proc = this.metadataFactory.getProcedure(sp.getGroup().getName());
            } catch (TranslatorException e) {
                 throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30486, e);
            }
        }
        if (!sp.isReadOnly()) {
            readOnly = false;
        }
        Class<?> returnType = null;
        List<Argument> translatedParameters = new ArrayList<Argument>();
        for (SPParameter param : sp.getParameters()) {
            Direction direction = Direction.IN;
            switch(param.getParameterType()) {
                case ParameterInfo.IN:
                    direction = Direction.IN;
                    break;
                case ParameterInfo.INOUT:
                    direction = Direction.INOUT;
                    break;
                case ParameterInfo.OUT:
                    direction = Direction.OUT;
                    break;
                case ParameterInfo.RESULT_SET:
                    continue; //already part of the metadata
                case ParameterInfo.RETURN_VALUE:
                    returnType = param.getClassType();
                    continue;
            }

            if (param.isUsingDefault() && BaseColumn.OMIT_DEFAULT.equalsIgnoreCase(metadataFactory.getMetadata().getExtensionProperty(param.getMetadataID(), BaseColumn.DEFAULT_HANDLING, false))) {
                continue;
            }

            ProcedureParameter metadataParam = metadataFactory.getParameter(param);
            //we can assume for now that all arguments will be literals, which may be multivalued
            org.teiid.language.Expression value = null;
            if (direction != Direction.OUT) {
                if (param.isVarArg()) {
                    ArrayImpl av = (ArrayImpl) ((Constant)param.getExpression()).getValue();
                    if (av != null) {
                        for (Object obj : av.getValues()) {
                            Argument arg = new Argument(direction, new Literal(obj, param.getClassType().getComponentType()), param.getClassType().getComponentType(), metadataParam);
                            translatedParameters.add(arg);
                        }
                    }
                    break;
                }
                value = translate(param.getExpression());
            }
            Argument arg = new Argument(direction, value, param.getClassType(), metadataParam);
            translatedParameters.add(arg);
        }

        Call call = new Call(removeSchemaName(sp.getProcedureName()), translatedParameters, proc);
        call.setReturnType(returnType);
        return call;
    }

    public NamedTable translate(GroupSymbol symbol) {
        String alias = null;
        String fullGroup = symbol.getOutputName();
        if(symbol.getOutputDefinition() != null) {
            alias = symbol.getOutputName();
            fullGroup = symbol.getOutputDefinition();
            if (remappedGroups != null) {
                GroupSymbol remappedGroup = remappedGroups.get(symbol.getMetadataID());
                if (remappedGroup != null && remappedGroup != symbol) {
                    fullGroup = remappedGroup.getName();
                }
            }
        }
        fullGroup = removeSchemaName(fullGroup);
        NamedTable group = new NamedTable(fullGroup, alias, null);
        try {
            group.setMetadataObject(metadataFactory.getGroup(symbol.getMetadataID()));
        } catch (QueryMetadataException e) {
             throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30487, e);
        } catch (TeiidComponentException e) {
             throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30488, e);
        }
        return group;
    }

    private String removeSchemaName(String fullGroup) {
        //remove the model name
        int index = fullGroup.indexOf(Symbol.SEPARATOR);
        if (index > 0) {
            fullGroup = fullGroup.substring(index + 1);
        }
        return fullGroup;
    }

    /* Batched Updates */
    BatchedUpdates translate(BatchedUpdateCommand command) {
        List<Command> updates = command.getUpdateCommands();
        List<org.teiid.language.Command> translatedUpdates = new ArrayList<org.teiid.language.Command>(updates.size());
        for (Iterator<Command> i = updates.iterator(); i.hasNext();) {
            translatedUpdates.add(translate(i.next()));
        }
        BatchedUpdates batchedUpdates = new BatchedUpdates(translatedUpdates);
        batchedUpdates.setSingleResult(command.isSingleResult());
        return batchedUpdates;
    }

    org.teiid.language.Limit translate(Limit limit) {
        if (limit == null) {
            return null;
        }
        int rowOffset = 0;
        if (limit.getOffset() != null) {
            Literal c1 = (Literal)translate(limit.getOffset());
            rowOffset = ((Integer)c1.getValue()).intValue();
        }
        Literal c2 = (Literal)translate(limit.getRowLimit());
        int rowLimit = Integer.MAX_VALUE;
        if (c2 != null) {
            rowLimit = ((Integer)c2.getValue()).intValue();
        }
        return new org.teiid.language.Limit(rowOffset, rowLimit);
    }

    public void setMaxInPredicateSize(int maxInCriteriaSize) {
        this.maxInCriteriaSize = maxInCriteriaSize;
    }

    public void setCommandContext(CommandContext commandContext) {
        this.commandContext = commandContext;
    }

    public static void correctNullOrdering(org.teiid.language.OrderBy orderBy, boolean supportsNullOrdering,
            NullOrder sourceNullOrder, NullOrder teiidNullOrder) {
        for (SortSpecification item : orderBy.getSortSpecifications()) {
            if (item.getNullOrdering() != null) {
                if (!supportsNullOrdering) {
                    item.setNullOrdering(null);
                }
            } else if (supportsNullOrdering) {
                //try to match the expected default
                if (item.getOrdering() == Ordering.ASC) {
                    if (teiidNullOrder == NullOrder.FIRST || teiidNullOrder == NullOrder.LOW) {
                        if (sourceNullOrder != NullOrder.FIRST && sourceNullOrder != NullOrder.LOW) {
                            item.setNullOrdering(NullOrdering.FIRST);
                        }
                    } else {
                        if (sourceNullOrder != NullOrder.LAST && sourceNullOrder != NullOrder.HIGH) {
                            item.setNullOrdering(NullOrdering.LAST);
                        }
                    }
                } else {
                    if (teiidNullOrder == NullOrder.LAST || teiidNullOrder == NullOrder.LOW) {
                        if (sourceNullOrder != NullOrder.LAST && sourceNullOrder != NullOrder.LOW) {
                            item.setNullOrdering(NullOrdering.LAST);
                        }
                    } else {
                        if (sourceNullOrder != NullOrder.FIRST && sourceNullOrder != NullOrder.HIGH) {
                            item.setNullOrdering(NullOrdering.FIRST);
                        }
                    }
                }
            }
        }
    }

    public void setSourceNullOrder(NullOrder sourceNullOrder) {
        this.sourceNullOrder = sourceNullOrder;
    }

    public void setSupportsNullOrdering(boolean supportsNullOrdering) {
        this.supportsNullOrdering = supportsNullOrdering;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

}
