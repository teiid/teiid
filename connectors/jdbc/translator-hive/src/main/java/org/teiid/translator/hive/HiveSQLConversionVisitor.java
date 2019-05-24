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
package org.teiid.translator.hive;

import static org.teiid.language.SQLConstants.Reserved.*;

import java.util.List;

import org.teiid.core.util.StringUtil;
import org.teiid.language.*;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.Join.JoinType;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.WindowFrame.BoundMode;
import org.teiid.language.WindowFrame.FrameBound;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.SQLConversionVisitor;

public class HiveSQLConversionVisitor extends SQLConversionVisitor {

    BaseHiveExecutionFactory baseHiveExecutionFactory;

    public HiveSQLConversionVisitor(BaseHiveExecutionFactory hef) {
        super(hef);
        this.baseHiveExecutionFactory = hef;
    }

    @Override
    public void visit(Join obj) {
        TableReference leftItem = obj.getLeftItem();
        TableReference rightItem = obj.getRightItem();
        JoinType joinType = obj.getJoinType();

        //impala only supports a left linear join
        if (baseHiveExecutionFactory.requiresLeftLinearJoin() && rightItem instanceof Join) {
            if (leftItem instanceof Join) {
                //TODO: this may need to be handled in the engine to inhibit pushdown
                throw new AssertionError("A left linear join structure is required: " + obj); //$NON-NLS-1$
            }

            //swap
            TableReference tr = leftItem;
            leftItem = rightItem;
            rightItem = tr;

            if (joinType == JoinType.RIGHT_OUTER_JOIN) {
                joinType = JoinType.LEFT_OUTER_JOIN;
            } else if (joinType == JoinType.LEFT_OUTER_JOIN) {
                joinType = JoinType.RIGHT_OUTER_JOIN;
            }
        }

        if(useParensForJoins() && leftItem instanceof Join) {
            buffer.append(Tokens.LPAREN);
            append(leftItem);
            buffer.append(Tokens.RPAREN);
        } else {
            append(leftItem);
        }
        buffer.append(Tokens.SPACE);

        switch(joinType) {
            case CROSS_JOIN:
                // Hive just works with "JOIN" keyword no inner or cross
                // fixed in - https://issues.apache.org/jira/browse/HIVE-2549
                buffer.append(CROSS);
                break;
            case FULL_OUTER_JOIN:
                buffer.append(FULL)
                      .append(Tokens.SPACE)
                      .append(OUTER);
                break;
            case INNER_JOIN:
                // Hive just works with "JOIN" keyword no inner or cross
                //buffer.append(INNER);
                break;
            case LEFT_OUTER_JOIN:
                buffer.append(LEFT)
                      .append(Tokens.SPACE)
                      .append(OUTER);
                break;
            case RIGHT_OUTER_JOIN:
                buffer.append(RIGHT)
                      .append(Tokens.SPACE)
                      .append(OUTER);
                break;
            default: buffer.append(UNDEFINED);
        }
        buffer.append(Tokens.SPACE)
              .append(JOIN)
              .append(Tokens.SPACE);

        if(rightItem instanceof Join && (useParensForJoins() || obj.getJoinType() == Join.JoinType.CROSS_JOIN)) {
            buffer.append(Tokens.LPAREN);
            append(rightItem);
            buffer.append(Tokens.RPAREN);
        } else {
            append(rightItem);
        }

        final Condition condition = obj.getCondition();
        if (condition != null) {
            buffer.append(Tokens.SPACE)
                  .append(ON)
                  .append(Tokens.SPACE);
            append(condition);
        }
    }

    public void addColumns(List<DerivedColumn> items) {
        if (items != null && items.size() != 0) {
            addColumn(items.get(0));
            for (int i = 1; i < items.size(); i++) {
                buffer.append(Tokens.COMMA)
                      .append(Tokens.SPACE);
                addColumn(items.get(i));
            }
        }
    }

    private void addColumn(DerivedColumn dc) {
        if (dc.getAlias() != null) {
            buffer.append(dc.getAlias());
        }
        else {
            Expression expr = dc.getExpression();
            if (expr instanceof ColumnReference) {
                buffer.append(((ColumnReference)expr).getName());
            }
            else {
                append(expr);
            }
        }
    }

    @Override
    public void visit(SetQuery obj) {
        //TODO: with hive 1.2, this handling is not necessary
        //even with hive 0.13 it's only partially necessary - for distinct
        if (obj.getWith() != null) {
            append(obj.getWith());
        }

        Select select =  obj.getProjectedQuery();
        startInlineView(select, !obj.isAll());

        appendSetChild(obj, obj.getLeftQuery(), false);

        appendSetOp(obj);

        appendSetChild(obj, obj.getRightQuery(), true);

        endInlineView(obj);
    }

    @Override
    protected String getLikeRegexString() {
        return "REGEXP"; //$NON-NLS-1$
    }

    private void endInlineView(QueryExpression obj) {
        buffer.append(Tokens.RPAREN);
        buffer.append(Tokens.SPACE);
        buffer.append("X__"); //$NON-NLS-1$

        OrderBy orderBy = obj.getOrderBy();
        if(orderBy != null) {
            buffer.append(Tokens.SPACE);
            append(orderBy);
        }

        Limit limit = obj.getLimit();
        if(limit != null) {
            buffer.append(Tokens.SPACE);
            append(limit);
        }
    }

    private void startInlineView(Select select, boolean distinct) {
        buffer.append(SELECT).append(Tokens.SPACE);
        if(distinct) {
            buffer.append(DISTINCT).append(Tokens.SPACE);
        }
        addColumns(select.getDerivedColumns());
        buffer.append(Tokens.SPACE);
        buffer.append(FROM).append(Tokens.SPACE);
        buffer.append(Tokens.LPAREN);
    }

    private void appendSetOp(SetQuery obj) {
        buffer.append(Tokens.SPACE);

        appendSetOperation(obj.getOperation());

        // UNION "ALL" always
        buffer.append(Tokens.SPACE);
        buffer.append(ALL);
        buffer.append(Tokens.SPACE);
    }

    private void appendSetChild(SetQuery obj, QueryExpression child, boolean right) {
        if (child instanceof Select || shouldNestSetChild(obj, child, right)) {
            appendSetQuery(obj, child, right);
        } else {
            //non-nested set op
            SetQuery setQuery = (SetQuery)child;

            append(setQuery.getLeftQuery());

            appendSetOp(setQuery);

            appendSetChild(setQuery, setQuery.getRightQuery(), true);
        }
    }

    @Override
    public void visit(Select obj) {
        if (obj.getOrderBy() != null) {
            //hive does not like order by using the full column references
            //this should be fine even with joins as the engine should alias the select columns
            for (SortSpecification spec : obj.getOrderBy().getSortSpecifications()) {
                if (spec.getExpression() instanceof ColumnReference) {
                    ColumnReference cr = (ColumnReference)spec.getExpression();
                    cr.setTable(null);
                }
            }
        }
        if (obj.isDistinct() && obj.getGroupBy() != null) {
            if (obj.getWith() != null) {
                append(obj.getWith());
            }
            if (obj.getOrderBy() == null) {
                boolean needsAliasing = false;
                List<DerivedColumn> derivedColumns = obj.getDerivedColumns();
                for (int i = 0; i < derivedColumns.size(); i++) {
                    DerivedColumn dc = derivedColumns.get(i);
                    if (dc.getAlias() == null) {
                        needsAliasing = true;
                        break;
                    }
                }
                if (needsAliasing) {
                    for (int i = 0; i < derivedColumns.size(); i++) {
                        DerivedColumn dc = derivedColumns.get(i);
                        dc.setAlias("c_" + i); //$NON-NLS-1$
                    }
                }
            }
            startInlineView(obj, obj.isDistinct());
            //remove the distinct from the inline view
            obj.setDistinct(false);
            super.visit(obj);
            endInlineView(obj);
            return;
        }
        super.visit(obj);
    }

    @Override
    protected void translateSQLType(Class<?> type, Object obj,
            StringBuilder valuesbuffer) {
        if (obj != null && type == TypeFacility.RUNTIME_TYPES.STRING) {
            String val = obj.toString();
            valuesbuffer.append(Tokens.QUOTE)
              .append(StringUtil.replaceAll(StringUtil.replaceAll(val, "\\", "\\\\"), "'", "\\'")) //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
              .append(Tokens.QUOTE);
        } else {
            super.translateSQLType(type, obj, valuesbuffer);
        }
    }

    @Override
    public void visit(Comparison obj) {
        if (baseHiveExecutionFactory.rewriteBooleanFunctions() && obj.getLeftExpression() instanceof Function
                && obj.getRightExpression() instanceof Literal
                && obj.getLeftExpression().getType() == TypeFacility.RUNTIME_TYPES.BOOLEAN) {
            Literal l = (Literal) obj.getRightExpression();
            Boolean val = (Boolean)l.getValue();
            Function leftExpression = (Function)obj.getLeftExpression();
            if ((Boolean.FALSE.equals(val) && obj.getOperator() == Operator.EQ) ||
                    (Boolean.TRUE.equals(val) && obj.getOperator() == Operator.NE)) {
                buffer.append(SQLConstants.Reserved.NOT);
                buffer.append(SQLConstants.Tokens.LPAREN);
                visit(leftExpression);
                buffer.append(SQLConstants.Tokens.RPAREN);
                return;
            } else if ((Boolean.TRUE.equals(val) && obj.getOperator() == Operator.EQ) ||
                    (Boolean.FALSE.equals(val) && obj.getOperator() == Operator.NE)) {
                visit(leftExpression);
                return;
            }
        }
        super.visit(obj);
    }

    @Override
    public void visit(WindowFrame windowFrame) {
        if (windowFrame.getEnd() == null) {
            windowFrame.setEnd(new FrameBound(BoundMode.CURRENT_ROW));
        }
        super.visit(windowFrame);
    }

}
