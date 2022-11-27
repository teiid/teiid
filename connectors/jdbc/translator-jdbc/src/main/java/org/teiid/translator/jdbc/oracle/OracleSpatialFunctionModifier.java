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
import java.util.List;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.Literal;
import org.teiid.translator.jdbc.FunctionModifier;


public class OracleSpatialFunctionModifier extends FunctionModifier {

    /**
     * If either of the first two parameters are a Literal String, then we need to put the literal itself in the SQL
     * to be passed to Oracle, without the tick marks
     */
    public List<?> translate(Function function) {
        List<Expression> params = function.getParameters();
        List<Object> objs = new ArrayList<Object>();
        objs.add(function.getName());
        objs.add("("); //$NON-NLS-1$
        addParamWithConversion(objs, params.get(0));
        objs.add(", "); //$NON-NLS-1$

        addParamWithConversion(objs, params.get(1));
        for (int i = 2; i < params.size(); i++) {
            objs.add(", "); //$NON-NLS-1$
            objs.add(params.get(i));
        }
        objs.add(")"); //$NON-NLS-1$
        return objs;
    }

    protected void addParamWithConversion(List<Object> objs,
                                          Expression expression) {
        if ((expression instanceof Literal)
                && (((Literal) expression).getValue() instanceof String)) {
            objs.add(((Literal) expression).getValue());
        } else {
            objs.add(expression);
        }
    }

}