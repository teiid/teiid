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

import org.teiid.language.Function;

/**
 * Modifier for cases where arguments need to be rearranged
 * or new text needs to be added.
 *
 * E.g.
 *
 * new FunctionModifier() {
 *     public List&lt;?&gt; translate(Function function) {
 *         return Arrays.asList(
 *             "SDO_RELATE(",
 *             function.getParameters().get(0),
 *             ", ",
 *             function.getParameters().get(1),
 *             ", 'mask=touch')"
 *         );
 *     }
 * }
 *
 * new TemplateFunctionModifier("SDO_RELATE(", 0, ", ", 1, ", 'mask=touch')")
 *
 */
public class TemplateFunctionModifier extends FunctionModifier {
    private final Object[] parts;

    public TemplateFunctionModifier(Object... parts) {
        this.parts = parts;
    }

    @Override
    public List<?> translate(Function function) {
        List<Object> objs = new ArrayList<Object>();
        for (Object part : parts) {
            if (part instanceof Integer) {
                objs.add(function.getParameters().get((Integer) part));
            } else {
                objs.add(part);
            }
        }
        return objs;
    }

}
