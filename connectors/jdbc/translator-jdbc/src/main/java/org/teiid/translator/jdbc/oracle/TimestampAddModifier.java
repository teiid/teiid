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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.language.Function;
import org.teiid.language.Literal;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.translator.jdbc.ExtractFunctionModifier;
import org.teiid.translator.jdbc.FunctionModifier;

public class TimestampAddModifier extends FunctionModifier {

    private static Map<String, String> INTERVAL_MAP = new HashMap<String, String>();

    static {
        INTERVAL_MAP.put(NonReserved.SQL_TSI_DAY, ExtractFunctionModifier.DAY);
        INTERVAL_MAP.put(NonReserved.SQL_TSI_HOUR, ExtractFunctionModifier.HOUR);
        INTERVAL_MAP.put(NonReserved.SQL_TSI_MINUTE, ExtractFunctionModifier.MINUTE);
        INTERVAL_MAP.put(NonReserved.SQL_TSI_MONTH, ExtractFunctionModifier.MONTH);
        INTERVAL_MAP.put(NonReserved.SQL_TSI_SECOND, ExtractFunctionModifier.SECOND);
        INTERVAL_MAP.put(NonReserved.SQL_TSI_YEAR, ExtractFunctionModifier.YEAR);
    }

    @Override
    public List<?> translate(Function function) {
        ArrayList<Object> result = new ArrayList<Object>();
        Literal intervalType = (Literal)function.getParameters().get(0);
        String interval = ((String)intervalType.getValue()).toUpperCase();
        //by capabilities this must be a literal integer
        int value = (Integer)((Literal)function.getParameters().get(1)).getValue();
        long adjustedValue = value;

        //handle the year/month case
        if (interval.equals(NonReserved.SQL_TSI_YEAR)) {
            interval = NonReserved.SQL_TSI_MONTH;
            adjustedValue = value*12L;
        }
        if (interval.equals(NonReserved.SQL_TSI_MONTH)) {
            interval = ExtractFunctionModifier.MONTH;
            result.add("ADD_MONTHS("); //$NON-NLS-1$
            result.add(function.getParameters().get(2));
            result.add(", "); //$NON-NLS-1$
            result.add(adjustedValue);
            result.add(")"); //$NON-NLS-1$
            return result;
        }

        result.add(function.getParameters().get(2));
        result.add(" + (INTERVAL '"); //$NON-NLS-1$
        String newInterval = INTERVAL_MAP.get(interval);
        if (newInterval != null) {
            result.add(value);
        } else if (interval.equals(NonReserved.SQL_TSI_QUARTER)) {
            newInterval = ExtractFunctionModifier.MONTH;
            adjustedValue = value*3L;
            result.add(adjustedValue);
        } else if (interval.equals(NonReserved.SQL_TSI_FRAC_SECOND)) {
            newInterval = ExtractFunctionModifier.SECOND;
            result.add(String.format("0.%09d", value)); //$NON-NLS-1$
            adjustedValue = 1;
        } else {
            newInterval = ExtractFunctionModifier.DAY;
            adjustedValue = value*7L;
            result.add(adjustedValue);
        }
        result.add("' "); //$NON-NLS-1$
        result.add(newInterval);
        result.add("("); //$NON-NLS-1$
        result.add(precision(Math.abs(adjustedValue)));
        result.add("))"); //$NON-NLS-1$
        return result;
    }

    static int precision(long value) {
        int precision = 1;
        while (value >= 10) {
            precision++;
            value /= 10;
        }
        return precision;
    }

}