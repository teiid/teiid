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

package org.teiid.translator.jdbc.postgresql;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.language.Literal;

public class LocateFunctionModifier extends org.teiid.translator.jdbc.LocateFunctionModifier {

    public LocateFunctionModifier(LanguageFactory factory) {
        super(factory);
    }

    @Override
    public List<?> translate(Function function) {
        modify(function);
        List<Object> parts = new ArrayList<Object>();
        List<Expression> params = function.getParameters();
        parts.add("position("); //$NON-NLS-1$
        parts.add(params.get(0));
        parts.add(" in "); //$NON-NLS-1$
        boolean useSubStr = false;
        if (params.size() == 3) {
            useSubStr = true;
            if (params.get(2) instanceof Literal && ((Literal)params.get(2)).getValue() instanceof Integer) {
                Integer value = (Integer)((Literal)params.get(2)).getValue();
                if (value <= 1) {
                    useSubStr = false;
                }
            }
        }
        if (useSubStr) {
            parts.add(0, "("); //$NON-NLS-1$
            parts.add("substring("); //$NON-NLS-1$
            parts.add(params.get(1));
            parts.add(" from "); //$NON-NLS-1$
            parts.add(params.get(2));
            parts.add("))"); //$NON-NLS-1$
            parts.add(" + "); //$NON-NLS-1$
            if (params.get(2) instanceof Literal && ((Literal)params.get(2)).getValue() instanceof Integer) {
                Integer value = (Integer)((Literal)params.get(2)).getValue();
                parts.add(value - 1);
            } else {
                parts.add(params.get(2));
                parts.add(" - 1"); //$NON-NLS-1$
            }
        } else {
            parts.add(params.get(1));
        }
        parts.add(")"); //$NON-NLS-1$
        return parts;
    }

}
