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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.translator.SourceSystemFunctions;


/**
 * Convert the YEAR/MONTH/DAY etc. function into an equivalent Extract function.
 * Format: EXTRACT(YEAR from Element) or EXTRACT(YEAR from DATE '2004-03-03')
 */
public class ExtractFunctionModifier extends FunctionModifier {
    public static final String YEAR = "YEAR"; //$NON-NLS-1$
    public static final String QUARTER = "QUARTER"; //$NON-NLS-1$
    public static final String MONTH = "MONTH"; //$NON-NLS-1$
    public static final String DAYOFYEAR = "DOY"; //$NON-NLS-1$
    public static final String DAY = "DAY"; //$NON-NLS-1$
    public static final String WEEK = "WEEK"; //$NON-NLS-1$
    public static final String DAYOFWEEK = "DOW"; //$NON-NLS-1$
    public static final String HOUR = "HOUR"; //$NON-NLS-1$
    public static final String MINUTE = "MINUTE"; //$NON-NLS-1$
    public static final String SECOND = "SECOND"; //$NON-NLS-1$
    public static final String MILLISECONDS = "MILLISECONDS"; //$NON-NLS-1$

    private static Map<String, String> FUNCTION_PART_MAP = new HashMap<String, String>();

    String castTarget;

    static {
        FUNCTION_PART_MAP.put(SourceSystemFunctions.WEEK, WEEK);
        FUNCTION_PART_MAP.put(SourceSystemFunctions.DAYOFWEEK, DAYOFWEEK);
        FUNCTION_PART_MAP.put(SourceSystemFunctions.DAYOFYEAR, DAYOFYEAR);
        FUNCTION_PART_MAP.put(SourceSystemFunctions.YEAR, YEAR);
        FUNCTION_PART_MAP.put(SourceSystemFunctions.QUARTER, QUARTER);
        FUNCTION_PART_MAP.put(SourceSystemFunctions.MONTH, MONTH);
        FUNCTION_PART_MAP.put(SourceSystemFunctions.DAYOFMONTH, DAY);
        FUNCTION_PART_MAP.put(SourceSystemFunctions.HOUR, HOUR);
        FUNCTION_PART_MAP.put(SourceSystemFunctions.MINUTE, MINUTE);
        FUNCTION_PART_MAP.put(SourceSystemFunctions.SECOND, SECOND);
    }

    public ExtractFunctionModifier() {
    }

    public ExtractFunctionModifier(String castTarget) {
        this.castTarget = castTarget;
    }

    public List<?> translate(Function function) {
        List<Expression> args = function.getParameters();
        List<Object> objs = new ArrayList<Object>();
        objs.add("EXTRACT("); //$NON-NLS-1$
        objs.add(FUNCTION_PART_MAP.get(function.getName().toLowerCase()));
        objs.add(Tokens.SPACE);
        objs.add(Reserved.FROM);
        objs.add(Tokens.SPACE);
        objs.add(args.get(0));
        objs.add(Tokens.RPAREN);
        //for pg - may not be needed for other dbs
        if (function.getName().toLowerCase().equals(SourceSystemFunctions.DAYOFWEEK)) {
            objs.add(0, Tokens.LPAREN);
            objs.add(" + 1)"); //$NON-NLS-1$
        }
        if (castTarget != null) {
            if (castTarget != null) {
                objs.add(0, "CAST("); //$NON-NLS-1$
            }
            objs.add(" AS "); //$NON-NLS-1$
            objs.add(castTarget);
            objs.add(")"); //$NON-NLS-1$
        }
        return objs;
    }
}
