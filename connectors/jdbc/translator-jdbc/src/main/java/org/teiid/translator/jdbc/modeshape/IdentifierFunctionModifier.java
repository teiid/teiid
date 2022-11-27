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

package org.teiid.translator.jdbc.modeshape;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.language.ColumnReference;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.NamedTable;
import org.teiid.language.SQLConstants;
import org.teiid.metadata.Column;
import org.teiid.translator.jdbc.FunctionModifier;


/**
 * Function to translate ColumnReferences to selector names
 * @since 7.1
 */
public class IdentifierFunctionModifier extends FunctionModifier {

    public List<?> translate(Function function) {

        List<Object> objs = new ArrayList<Object>();

        List<Expression> parms = function.getParameters();

        objs.add(function.getName().substring(function.getName().indexOf('_') + 1));
        objs.add(SQLConstants.Tokens.LPAREN);

        for (Iterator<Expression> iter = parms.iterator(); iter.hasNext();)
        {
            Expression expr = iter.next();
            if (expr instanceof ColumnReference) {
                boolean dotAll = false;
                boolean useSelector = false;
                ColumnReference cr = (ColumnReference)expr;
                Column c = cr.getMetadataObject();
                if (c != null) {
                    if ("\"mode:properties\"".equalsIgnoreCase(c.getSourceName())) { //$NON-NLS-1$
                        dotAll = true;
                        useSelector = true;
                    } else if ("\"jcr:path\"".equalsIgnoreCase(c.getSourceName())) { //$NON-NLS-1$
                        useSelector = true;
                    }
                }
                if (useSelector) {
                    NamedTable nt = ((ColumnReference)expr).getTable();
                    if (nt.getCorrelationName() != null) {
                        objs.add(nt.getCorrelationName());
                    } else {
                        objs.add(nt);
                    }
                } else {
                    objs.add(expr);
                }
                if (dotAll) {
                    objs.add(".*"); //$NON-NLS-1$
                }
            } else {
                objs.add(expr);
            }
            if (iter.hasNext()) {
                objs.add(", "); //$NON-NLS-1$
            }
         }

        objs.add(SQLConstants.Tokens.RPAREN);
        return objs;
    }

}
