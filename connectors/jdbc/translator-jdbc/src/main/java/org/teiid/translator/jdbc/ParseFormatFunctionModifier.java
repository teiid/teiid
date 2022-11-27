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

package org.teiid.translator.jdbc;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.Literal;

public abstract class ParseFormatFunctionModifier extends FunctionModifier {

    protected String prefix;

    public ParseFormatFunctionModifier(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public List<?> translate(Function function) {
        if (!(function.getParameters().get(1) instanceof Literal)) {
            return null; //shouldn't happen
        }
        Literal l = (Literal)function.getParameters().get(1);
        List<Object> result = new ArrayList<Object>();
        result.add(prefix);
        translateFormat(result, function.getParameters().get(0), (String)l.getValue());
        result.add(")"); //$NON-NLS-1$
        return result;
    }

    protected void translateFormat(List<Object> result, Expression expression,
            String value) {
        result.add(expression);
        result.add(", "); //$NON-NLS-1$
        result.add(translateFormat(value));
    }

    abstract protected Object translateFormat(String format);
}