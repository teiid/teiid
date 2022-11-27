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
 * Wrap a function in standard JDBC escape syntax.  In some cases, the
 * driver can then convert to the correct database syntax for us.
 * @since 5.0
 */
public class EscapeSyntaxModifier extends FunctionModifier {

    public List<?> translate(Function function) {
        List<Object> objs = new ArrayList<Object>();
        objs.add("{fn "); //$NON-NLS-1$
        objs.add(function);
        objs.add("}"); //$NON-NLS-1$
        return objs;
    }

}
