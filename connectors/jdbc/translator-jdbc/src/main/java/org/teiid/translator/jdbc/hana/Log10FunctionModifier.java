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

package org.teiid.translator.jdbc.hana;

import java.util.List;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.FunctionModifier;


public class Log10FunctionModifier extends FunctionModifier {

    private LanguageFactory languageFactory;

    public Log10FunctionModifier(LanguageFactory languageFactory) {
        this.languageFactory = languageFactory;
    }

    @Override
    public List<?> translate(Function function) {
        function.setName("log"); //$NON-NLS-1$

        List<Expression> args = function.getParameters();
        args.add(args.get(0));
        args.set(0, languageFactory.createLiteral(new Integer(10), TypeFacility.RUNTIME_TYPES.INTEGER));
        return null;
    }

}
