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

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.translator.jdbc.FunctionModifier;


public class HanaSpatialFunctionModifier extends FunctionModifier {

    /**
     * Most geospatial functions in HANA are called from the geometry object or an equivalent expression.
     * For example, &lt;geometry-expression&gt;.ST_SRID() or &lt;geometry-expression&gt;.ST_Relate(&lt;geo2&gt;). This method
     * will take the argument(s) to the Teiid spatial function and move the first argument to precede
     * the function name.
     */
    public List<?> translate(Function function) {
        List<Expression> params = function.getParameters();
        List<Object> objs = new ArrayList<Object>();

        Expression exp1 = params.get(0);

        objs.add(exp1+"."+function.getName()); //$NON-NLS-1$
        objs.add("("); //$NON-NLS-1$
        if (params.size()>1){
            objs.add(params.get(1));
        }
        objs.add(")"); //$NON-NLS-1$
        return objs;
    }

}