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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.TypeFacility;

/**
 * Adds mod (remainder) support for non-integral types
 */
public class ModFunctionModifier extends AliasModifier {

    private Set<Class<?>> supportedTypes = new HashSet<Class<?>>(Arrays.asList(TypeFacility.RUNTIME_TYPES.INTEGER, TypeFacility.RUNTIME_TYPES.LONG));

    private LanguageFactory langFactory;

    public ModFunctionModifier(String modFunction, LanguageFactory langFactory) {
        this(modFunction, langFactory, null);
    }

    public ModFunctionModifier(String modFunction, LanguageFactory langFactory, Collection<? extends Class<?>> supportedTypes) {
        super(modFunction);
        this.langFactory = langFactory;
        if (supportedTypes != null) {
            this.supportedTypes.addAll(supportedTypes);
        }
    }

    @Override
    public List<?> translate(Function function) {
        List<Expression> expressions = function.getParameters();
        Class<?> type = function.getType();
        if (supportedTypes.contains(type)) {
            modify(function);
            return null;
        }
        //x % y => x - sign(x) * floor(abs(x / y)) * y
        Function divide = langFactory.createFunction(SourceSystemFunctions.DIVIDE_OP, new ArrayList<Expression>(expressions), type);

        Function abs = langFactory.createFunction(SourceSystemFunctions.ABS, Arrays.asList(divide), type);

        Function floor = langFactory.createFunction(SourceSystemFunctions.FLOOR, Arrays.asList(abs), type);

        Function sign = langFactory.createFunction(SourceSystemFunctions.SIGN, Arrays.asList(expressions.get(0)), type);

        List<? extends Expression> multArgs = Arrays.asList(sign, floor, langFactory.createFunction(SourceSystemFunctions.ABS, Arrays.asList(expressions.get(1)), type));
        Function mult = langFactory.createFunction(SourceSystemFunctions.MULTIPLY_OP, multArgs, type);

        List<Expression> minusArgs = Arrays.asList(expressions.get(0), mult);

        return Arrays.asList(langFactory.createFunction(SourceSystemFunctions.SUBTRACT_OP, minusArgs, type));
    }

}
