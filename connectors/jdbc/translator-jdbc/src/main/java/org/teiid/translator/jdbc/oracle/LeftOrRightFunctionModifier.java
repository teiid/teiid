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

import java.util.Arrays;
import java.util.List;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.FunctionModifier;


/**
 * Convert left(string, count) --&gt; substr(string, 1, count)
 * or right(string, count) --&gt; substr(string, -1 * count) - we lack a way to express a unary negation
 */
public class LeftOrRightFunctionModifier extends FunctionModifier {
    private LanguageFactory langFactory;

    public LeftOrRightFunctionModifier(LanguageFactory langFactory) {
        this.langFactory = langFactory;
    }

    @Override
    public List<?> translate(Function function) {
        List<Expression> args = function.getParameters();
        Function func = null;

        if (function.getName().equalsIgnoreCase("left")) { //$NON-NLS-1$
            func = langFactory.createFunction(SourceSystemFunctions.SUBSTRING,
                Arrays.asList(
                    args.get(0),
                    langFactory.createLiteral(Integer.valueOf(1), TypeFacility.RUNTIME_TYPES.INTEGER),
                    args.get(1)),
                    String.class);
        } else if (function.getName().equalsIgnoreCase("right")) { //$NON-NLS-1$
            Function negIndex = langFactory.createFunction("*",  //$NON-NLS-1$
                Arrays.asList(langFactory.createLiteral(Integer.valueOf(-1), TypeFacility.RUNTIME_TYPES.INTEGER), args.get(1)),
                Integer.class);

            func = langFactory.createFunction(SourceSystemFunctions.SUBSTRING,
                Arrays.asList(
                    args.get(0),
                    negIndex),
                    String.class);
        }

        return Arrays.asList(func);
    }
}
