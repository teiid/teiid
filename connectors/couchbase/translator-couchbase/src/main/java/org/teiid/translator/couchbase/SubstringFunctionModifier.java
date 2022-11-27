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

package org.teiid.translator.couchbase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.Comparison;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.Literal;
import org.teiid.language.SearchedCase;
import org.teiid.language.SearchedWhenClause;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;

public class SubstringFunctionModifier extends AliasModifier {

    public SubstringFunctionModifier() {
        super("substr"); //$NON-NLS-1$
    }

    @Override
    public List<?> translate(Function function) {
        this.modify(function);

        Expression from = function.getParameters().get(1);
        Boolean isFromNegative = isNegative(from);

        Function length = new Function(
                SourceSystemFunctions.LENGTH,
                Arrays.asList(function.getParameters().get(0)),
                TypeFacility.RUNTIME_TYPES.INTEGER);

        if (function.getParameters().size() == 2 && (isFromNegative == null || isFromNegative)) {
            //couchbase does not handle default length with a negative from index
            function.getParameters().add(length);
        }

        if (function.getParameters().size() == 3) {
              //case when length > LENGTH(string) - start + 1 then LENGTH(string) - start + 1 case when length > 0 then length end
            Expression forLength = function.getParameters().get(2);
            List<SearchedWhenClause> clauses = new ArrayList<SearchedWhenClause>(2);
            Boolean isNegative = isNegative(forLength);

            Expression adjustedFrom = from;
            if (isFromNegative == null || isFromNegative) {
                adjustedFrom = new SearchedCase(Arrays.asList(new SearchedWhenClause(new Comparison(from,  new Literal(0, TypeFacility.RUNTIME_TYPES.INTEGER), Operator.LT), new Function(
                    SourceSystemFunctions.ADD_OP,
                    Arrays.asList(
                            new Function(SourceSystemFunctions.ADD_OP, Arrays.asList(length, new Literal(1, TypeFacility.RUNTIME_TYPES.INTEGER)),TypeFacility.RUNTIME_TYPES.INTEGER),
                            from),
                    TypeFacility.RUNTIME_TYPES.INTEGER))),
                    from, TypeFacility.RUNTIME_TYPES.INTEGER);
            }

            Expression maxLength = new Function(
                    SourceSystemFunctions.SUBTRACT_OP,
                    Arrays.asList(length,
                                new Function(
                                    SourceSystemFunctions.SUBTRACT_OP,
                                    Arrays.asList(
                                            adjustedFrom,
                                            new Literal(1, TypeFacility.RUNTIME_TYPES.INTEGER)),
                                    TypeFacility.RUNTIME_TYPES.INTEGER)),
                    TypeFacility.RUNTIME_TYPES.INTEGER);
            clauses.add(new SearchedWhenClause(new Comparison(forLength, maxLength, Operator.GT), maxLength));
            Expression defaultExpr = null;
            if (isNegative == null) {
                clauses.add(new SearchedWhenClause(new Comparison(forLength, new Literal(0, TypeFacility.RUNTIME_TYPES.INTEGER), Operator.GT), forLength));
            } else if (isNegative) {
                //TODO: could be done in the rewriter
                return Arrays.asList(new Literal(null, TypeFacility.RUNTIME_TYPES.STRING));
            } else {
                defaultExpr = forLength;
            }
            SearchedCase sc = new SearchedCase(clauses,
                    defaultExpr, TypeFacility.RUNTIME_TYPES.INTEGER);
            function.getParameters().set(2, sc);
        }

        Expression adjustedFrom = function.getParameters().get(1);
        if (isFromNegative == null) {
            //case when start > 0 then start - 1 else start end
            SearchedCase sc = new SearchedCase(Arrays.asList(new SearchedWhenClause(new Comparison(adjustedFrom, new Literal(0, TypeFacility.RUNTIME_TYPES.INTEGER), Operator.GT),
                    new Function(SourceSystemFunctions.SUBTRACT_OP, Arrays.asList(adjustedFrom, new Literal(1, TypeFacility.RUNTIME_TYPES.INTEGER)),TypeFacility.RUNTIME_TYPES.INTEGER))),
                    from, TypeFacility.RUNTIME_TYPES.INTEGER);
            function.getParameters().set(1, sc);
        } else if (!isFromNegative){
            function.getParameters().set(1, new Function(SourceSystemFunctions.SUBTRACT_OP, Arrays.asList(adjustedFrom, new Literal(1, TypeFacility.RUNTIME_TYPES.INTEGER)),TypeFacility.RUNTIME_TYPES.INTEGER));
        }

        return null;
    }

    private Boolean isNegative(Expression ex) {
        Boolean isNegative = null;
        if (ex instanceof Literal) {
            Literal l = (Literal)ex;
            int value = (Integer)l.getValue();
            isNegative = value < 0;
        }
        return isNegative;
    }

}
