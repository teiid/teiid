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

import java.util.List;

import org.teiid.language.*;

public class AliasModifier extends FunctionModifier {
    // The alias to use
    protected String alias;

    /**
     * Constructor that takes the alias to use for functions.
     * @param alias The alias to replace the incoming function name with
     */
    public AliasModifier(String alias) {
        this.alias = alias;
    }

    @Override
    public List<?> translate(Function function) {
        modify(function);
        return null;
    }

    protected void modify(Function function) {
        function.setName(alias);
    }

}
