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

package org.teiid.translator.yahoo;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.language.Expression;
import org.teiid.language.In;
import org.teiid.language.Literal;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.translator.TranslatorException;


/**
 */
public class TickerCollectorVisitor extends HierarchyVisitor {

    private Set<String> tickers = new HashSet<String>();
    private TranslatorException exception;

    public Set<String> getTickers() {
        return this.tickers;
    }

    public TranslatorException getException() {
        return this.exception;
    }

    public void visit(Comparison obj) {
        Expression expr = obj.getRightExpression();
        addTickerFromExpression(expr);
    }

    public void visit(In obj) {
        List<Expression> exprs = obj.getRightExpressions();
        for (Expression expr : exprs) {
            addTickerFromExpression(expr);
        }
    }

    private void addTickerFromExpression(Expression expr) {
        if(expr instanceof Literal) {
            Literal literal = (Literal) expr;
            if(literal.getType() == String.class) {
                String ticker = (String) literal.getValue();
                this.tickers.add(ticker.toUpperCase());
            } else {
                this.exception = new TranslatorException(YahooPlugin.Util.getString("TickerCollectorVisitor.Unexpected_type", literal.getType().getName())); //$NON-NLS-1$
            }
        } else {
            this.exception = new TranslatorException(YahooPlugin.Util.getString("TickerCollectorVisitor.Unexpected_expression", expr)); //$NON-NLS-1$
        }

    }

    public static Set<String> getTickers(Condition crit) throws TranslatorException {
        TickerCollectorVisitor visitor = new TickerCollectorVisitor();
        crit.acceptVisitor(visitor);

        if(visitor.getException() != null) {
            throw visitor.getException();
        }
        return visitor.getTickers();
    }

}
