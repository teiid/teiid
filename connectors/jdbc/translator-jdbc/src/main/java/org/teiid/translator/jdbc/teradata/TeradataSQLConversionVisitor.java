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
package org.teiid.translator.jdbc.teradata;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.AndOr;
import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.language.Expression;
import org.teiid.language.In;
import org.teiid.language.LanguageFactory;
import org.teiid.language.Literal;
import org.teiid.language.AndOr.Operator;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.translator.jdbc.SQLConversionVisitor;

public class TeradataSQLConversionVisitor extends SQLConversionVisitor {

    public TeradataSQLConversionVisitor(TeradataExecutionFactory ef) {
        super(ef);
    }

    @Override
    public void visit(In obj) {
        List<Expression> exprs = obj.getRightExpressions();

        boolean decompose = false;
        for (Expression expr:exprs) {
            if (!(expr instanceof Literal)) {
                decompose = true;
                break;
            }
        }

        if (decompose) {
            List<Expression> literals = new ArrayList<Expression>();
            Comparison.Operator opCode = obj.isNegated()?Comparison.Operator.NE:Comparison.Operator.EQ;
            if (exprs.size() > 1) {
                Condition left = null;
                for (Expression expr : obj.getRightExpressions()) {
                    if (expr instanceof Literal) {
                        literals.add(expr);
                    } else {
                        if (left == null) {
                            left = LanguageFactory.INSTANCE.createCompareCriteria(opCode, obj.getLeftExpression(), expr);
                        } else {
                            left = LanguageFactory.INSTANCE.createAndOr(obj.isNegated()?Operator.AND:Operator.OR, left, LanguageFactory.INSTANCE.createCompareCriteria(opCode, obj.getLeftExpression(), expr));
                        }
                    }
                }
                if (!literals.isEmpty()) {
                    left = LanguageFactory.INSTANCE.createAndOr(obj.isNegated()?Operator.AND:Operator.OR, left, new In(obj.getLeftExpression(), literals, obj.isNegated()));
                }
                buffer.append(Tokens.LPAREN);
                super.visit((AndOr)left);
                buffer.append(Tokens.RPAREN);
            }
            else {
                super.visit(LanguageFactory.INSTANCE.createCompareCriteria(opCode, obj.getLeftExpression(), exprs.get(0)));
            }
        }
        else {
            super.visit(obj);
        }
    }
}
