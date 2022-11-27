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
 * Convert the MONTHNAME etc. function into an equivalent Oracle function.
 * Format: to_char(timestampvalue/dayvalue, 'Month'/'Day')
 */
public class MonthOrDayNameFunctionModifier extends FunctionModifier {
    private LanguageFactory langFactory;
    private String format;

    public MonthOrDayNameFunctionModifier(LanguageFactory langFactory, String format) {
        this.langFactory = langFactory;
        this.format = format;
    }

    @Override
    public List<?> translate(Function function) {
        List<Expression> args = function.getParameters();

        Function func = langFactory.createFunction("TO_CHAR",  //$NON-NLS-1$
            Arrays.asList(
                args.get(0),
                langFactory.createLiteral(format, TypeFacility.RUNTIME_TYPES.STRING)),
            TypeFacility.RUNTIME_TYPES.STRING);

        // For some reason, these values have trailing spaces
        Function trimFunc = langFactory.createFunction(SourceSystemFunctions.RTRIM,
            Arrays.asList( func ), TypeFacility.RUNTIME_TYPES.STRING);

        return Arrays.asList(trimFunc);
    }
}
