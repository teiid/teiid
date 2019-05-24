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
package org.teiid.translator.jdbc.pi;

import static org.teiid.language.SQLConstants.Reserved.*;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.*;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.Join.JoinType;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.SubqueryComparison.Quantifier;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.Procedure;
import org.teiid.translator.jdbc.JDBCPlugin;
import org.teiid.translator.jdbc.SQLConversionVisitor;

public class PISQLConversionVisitor extends SQLConversionVisitor {

    private static final String APPLY = "APPLY";
    PIExecutionFactory executionFactory;

    public PISQLConversionVisitor(PIExecutionFactory hef) {
        super(hef);
        this.executionFactory = hef;
    }

    @Override
    public void visit(SetQuery obj) {
        Limit limit = obj.getLimit();
        if(limit != null) {
            buffer.append(SELECT);
            buffer.append(Tokens.SPACE);
            append(limit);
            buffer.append(Tokens.SPACE);
            buffer.append(Tokens.ALL_COLS);
            buffer.append(Tokens.SPACE);
            buffer.append(FROM);
            buffer.append(Tokens.SPACE);
            buffer.append(Tokens.LPAREN);
        }

        if (obj.getWith() != null) {
            append(obj.getWith());
        }
        appendSetQuery(obj, obj.getLeftQuery(), false);

        buffer.append(Tokens.SPACE);

        appendSetOperation(obj.getOperation());

        if(obj.isAll()) {
            buffer.append(Tokens.SPACE);
            buffer.append(ALL);
        }
        buffer.append(Tokens.SPACE);

        appendSetQuery(obj, obj.getRightQuery(), true);

        OrderBy orderBy = obj.getOrderBy();
        if(orderBy != null) {
            buffer.append(Tokens.SPACE);
            append(orderBy);
        }

        if(limit != null) {
            buffer.append(Tokens.RPAREN);
            buffer.append(Tokens.SPACE);
            buffer.append("_X_");
        }
    }

    @Override
    public void visit(Join obj) {
        TableReference leftItem = obj.getLeftItem();
        TableReference rightItem = obj.getRightItem();
        JoinType joinType = obj.getJoinType();

        boolean ignoreOnClause = false;
        Condition condition = obj.getCondition();

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
                if (hasLateralJoin(rightItem)) {
                    if (!isTVF(rightItem)) {
                        throw new TeiidRuntimeException(JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11025, rightItem));
                    }
                    buffer.append(CROSS)
                        .append(Tokens.SPACE)
                        .append(APPLY)
                        .append(Tokens.SPACE);
                    ignoreOnClause = true;
                } else {
                    // "a cross join b" is the same as "a inner join b on (1 = 1)"
                    buffer.append(INNER)
                        .append(Tokens.SPACE)
                        .append(JOIN)
                        .append(Tokens.SPACE);
                    Literal e1 = LanguageFactory.INSTANCE.createLiteral(new Integer(1), Integer.class);
                    Comparison criteria = new Comparison(e1, e1, Operator.EQ);
                    condition = criteria;
                }
                break;
            case FULL_OUTER_JOIN:
                // should not get here as capabilities are turned off.
                throw new TeiidRuntimeException(JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11024, "FULL OUTER"));
            case INNER_JOIN:
                buffer.append(INNER)
                    .append(Tokens.SPACE)
                    .append(JOIN)
                    .append(Tokens.SPACE);
                break;
            case LEFT_OUTER_JOIN:
                if (!hasLateralJoin(leftItem) && !hasLateralJoin(rightItem)) {
                    buffer.append(LEFT)
                      .append(Tokens.SPACE)
                      .append(OUTER)
                      .append(Tokens.SPACE)
                      .append(JOIN)
                      .append(Tokens.SPACE);
                } else if (hasLateralJoin(leftItem)) {
                    throw new TeiidRuntimeException(JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11024, "RIGHT OUTER APPLY"));
                } else if (hasLateralJoin(rightItem)) {
                    if (!isTVF(rightItem)) {
                        throw new TeiidRuntimeException(JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11025, rightItem));
                    }
                    buffer.append(OUTER)
                    .append(Tokens.SPACE)
                    .append(APPLY)
                    .append(Tokens.SPACE);
                    ignoreOnClause = true;
                }
                break;
            case RIGHT_OUTER_JOIN:
                // right outer is never pushed, so we should probably never get here.
                throw new TeiidRuntimeException(JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11024, "RIGHT OUTER"));
            default:
                buffer.append(UNDEFINED);
        }

        if(rightItem instanceof Join && (useParensForJoins() || obj.getJoinType() == Join.JoinType.CROSS_JOIN)) {
            buffer.append(Tokens.LPAREN);
            append(hasLateralJoin(rightItem)?unwrap(rightItem):rightItem);
            buffer.append(Tokens.RPAREN);
        } else {
            append(hasLateralJoin(rightItem)?unwrap(rightItem):rightItem);
        }

        if (condition != null && !ignoreOnClause) {
            buffer.append(Tokens.SPACE)
              .append(ON)
              .append(Tokens.SPACE);
            append(condition);
        }
    }

    private TableReference unwrap(TableReference table) {
        if (table instanceof DerivedTable) {
            return table;
        } else if (table instanceof NamedProcedureCall) {
            NamedProcedureCall npc = (NamedProcedureCall) table;
            return npc.getCall();
        }
        return null;
    }

    protected void appendCallStart(Call call) {
        if (!isTVF(call.getMetadataObject())) {
            buffer.append(EXEC)
                  .append(Tokens.SPACE);
        }
    }

    @Override
    public void visit(ColumnReference obj) {
        if (obj.getMetadataObject() != null) {
            ColumnSet<?> cs = obj.getMetadataObject().getParent();
            if (cs.getParent() instanceof Procedure && isTVF((Procedure)cs.getParent())) {
                this.shortNameOnly = true;
                super.visit(obj);
                this.shortNameOnly = false;
            } else {
                super.visit(obj);
            }
        } else {
            super.visit(obj);
        }
    }

    @Override
    public void visit(DerivedTable obj) {
        buffer.append(Tokens.LPAREN);
        append(obj.getQuery());
        buffer.append(Tokens.RPAREN);
        buffer.append(Tokens.SPACE);
        if(useAsInGroupAlias()) {
            buffer.append(AS);
            buffer.append(Tokens.SPACE);
        }
        buffer.append(obj.getCorrelationName());
    }

    protected void appendLateralKeyword() {
        buffer.append(LATERAL);
    }

    @Override
    public void visit(NamedProcedureCall obj) {
        buffer.append(Tokens.LPAREN);
        append(obj.getCall());
        buffer.append(Tokens.RPAREN);
        buffer.append(Tokens.SPACE);
        if(useAsInGroupAlias()) {
            buffer.append(AS);
            buffer.append(Tokens.SPACE);
        }
        buffer.append(obj.getCorrelationName());
    }

    private boolean hasLateralJoin(TableReference table) {
        if (table instanceof DerivedTable) {
            return ((DerivedTable)table).isLateral();
        } else if (table instanceof NamedProcedureCall) {
            return ((NamedProcedureCall)table).isLateral();
        }
        return false;
    }

    private boolean isTVF(TableReference table) {
        if (table instanceof NamedTable) {
            String value = ((NamedTable)table).getMetadataObject().getProperty(PIMetadataProcessor.TVF, false);
            return Boolean.parseBoolean(value);
        } else if (table instanceof NamedProcedureCall) {
            String value = ((NamedProcedureCall)table).getCall().getMetadataObject().getProperty(PIMetadataProcessor.TVF, false);
            return Boolean.parseBoolean(value);
        }
        return false;
    }

    private boolean isTVF(Procedure proc) {
        String value = proc.getProperty(PIMetadataProcessor.TVF, false);
        return Boolean.parseBoolean(value);
    }

    @Override
    protected void appendQuantifier(SubqueryComparison obj) {
        if(obj.getQuantifier() == Quantifier.SOME) {
            buffer.append("ANY"); //$NON-NLS-1$
        } else {
            super.appendQuantifier(obj);
        }
    }

    @Override
    public void visit(Comparison obj) {
        if (allowImplictConversion(obj)) {
            Function left = (Function) obj.getLeftExpression();
            obj.setLeftExpression(left.getParameters().get(0));
            Function right = (Function) obj.getRightExpression();
            obj.setRightExpression(right.getParameters().get(0));
        }
        super.visit(obj);
    }

    private boolean isConvertFunctionWithSimpleExpression(Expression expr) {
        if (expr instanceof Function && ((Function) expr).getName().equals("convert")) {
            Function f = (Function) expr;
            Expression param = f.getParameters().get(0);
            if (param instanceof ColumnReference) {
                return true;
            } else if (param instanceof Literal) {
                return true;
            }
        }
        return false;
    }

    private boolean allowImplictConversion(Comparison obj) {
        if (isConvertFunctionWithSimpleExpression(obj.getLeftExpression())
                && isConvertFunctionWithSimpleExpression(obj.getRightExpression())) {
            String left = getParameterDataType(obj.getLeftExpression());
            String right = getParameterDataType(obj.getRightExpression());
            if (left.equals(DataTypeManager.DefaultDataTypes.INTEGER)
                    && right.equals(DataTypeManager.DefaultDataTypes.FLOAT)) {
                return true;
            }
            if (left.equals(DataTypeManager.DefaultDataTypes.FLOAT)
                    && right.equals(DataTypeManager.DefaultDataTypes.INTEGER)) {
                return true;
            }
        }
        return false;
    }

    private String getParameterDataType(Expression expr) {
        Function f = (Function) expr;
        Expression param = f.getParameters().get(0);

        if (param instanceof ColumnReference) {
            return ((ColumnReference) param).getMetadataObject().getRuntimeType();
        } else if (param instanceof Literal) {
            return DataTypeManager.getDataTypeName(((Literal) param).getType());
        }
        return null;
    }
}
