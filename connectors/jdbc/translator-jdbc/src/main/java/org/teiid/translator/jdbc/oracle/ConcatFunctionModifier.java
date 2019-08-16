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

package org.teiid.translator.jdbc.oracle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.AndOr.Operator;
import org.teiid.language.Condition;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.language.Literal;
import org.teiid.language.SearchedWhenClause;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.FunctionModifier;



/**
 * This Function modifier used to support ANSI concat on Oracle 9i.
 * <code>
 * CONCAT(a, b) ==&gt; CASE WHEN (a is NULL OR b is NULL) THEN NULL ELSE CONCAT(a, b)
 * </code>
 */
public class ConcatFunctionModifier extends FunctionModifier {
    private LanguageFactory langFactory;

    /**
     * @param langFactory
     */
    public ConcatFunctionModifier(LanguageFactory langFactory) {
        this.langFactory = langFactory;
    }

    @Override
    public List<?> translate(Function function) {
        Expression a = function.getParameters().get(0);
        Expression b = function.getParameters().get(1);
        List<Condition> crits = new ArrayList<Condition>();

        Literal nullValue = langFactory.createLiteral(null, TypeFacility.RUNTIME_TYPES.STRING);
        if (isNull(a)) {
            return Arrays.asList(nullValue);
        } else if (!isNotNull(a)) {
            crits.add(langFactory.createIsNullCriteria(a, false));
        }
        if (isNull(b)) {
            return Arrays.asList(nullValue);
        } else if (!isNotNull(b)) {
            crits.add(langFactory.createIsNullCriteria(b, false));
        }

        Condition crit = null;

        if (crits.isEmpty()) {
            return null;
        } else if (crits.size() == 1) {
            crit = crits.get(0);
        } else {
            crit = langFactory.createAndOr(Operator.OR, crits.get(0), crits.get(1));
        }
        List<SearchedWhenClause> cases = Arrays.asList(langFactory.createSearchedWhenCondition(crit, nullValue));
        return Arrays.asList(langFactory.createSearchedCaseExpression(cases, function, TypeFacility.RUNTIME_TYPES.STRING));
    }

    public static boolean isNotNull(Expression expr) {
        if (expr instanceof Literal) {
            Literal literal = (Literal)expr;
            return literal.getValue() != null;
        }
        if (expr instanceof Function) {
            Function function = (Function)expr;
            if (function.getName().equalsIgnoreCase("NVL") || function.getName().equalsIgnoreCase(SourceSystemFunctions.IFNULL)) { //$NON-NLS-1$
                return isNotNull(function.getParameters().get(1));
            }
        }
        return false;
    }

    private boolean isNull(Expression expr) {
        if (expr instanceof Literal) {
            Literal literal = (Literal)expr;
            return literal.getValue() == null;
        }
        return false;
    }

}
