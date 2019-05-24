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

package org.teiid.translator.jdbc.hsql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.language.Literal;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.translator.jdbc.ExtractFunctionModifier;
import org.teiid.translator.jdbc.FunctionModifier;

public class AddDiffModifier extends FunctionModifier {

    private static Map<String, String> INTERVAL_MAP = new HashMap<String, String>();

    static {
        INTERVAL_MAP.put(NonReserved.SQL_TSI_DAY, ExtractFunctionModifier.DAY);
        INTERVAL_MAP.put(NonReserved.SQL_TSI_HOUR, ExtractFunctionModifier.HOUR);
        INTERVAL_MAP.put(NonReserved.SQL_TSI_MINUTE, ExtractFunctionModifier.MINUTE);
        INTERVAL_MAP.put(NonReserved.SQL_TSI_MONTH, ExtractFunctionModifier.MONTH);
        INTERVAL_MAP.put(NonReserved.SQL_TSI_SECOND, ExtractFunctionModifier.SECOND);
        INTERVAL_MAP.put(NonReserved.SQL_TSI_YEAR, ExtractFunctionModifier.YEAR);
    }

    private boolean add;
    private boolean supportsQuarter;
    private boolean literalPart = true;

    public AddDiffModifier(boolean add, LanguageFactory factory) {
        this.add = add;
    }

    public AddDiffModifier supportsQuarter(boolean b) {
        this.supportsQuarter = b;
        return this;
    }

    public AddDiffModifier literalPart(boolean b) {
        this.literalPart = b;
        return this;
    }

    @Override
    public List<?> translate(Function function) {
        ArrayList<Object> result = new ArrayList<Object>();
        if (add) {
            result.add("dateadd("); //$NON-NLS-1$
        } else {
            result.add("datediff("); //$NON-NLS-1$
        }
        for (int i = 0; i < function.getParameters().size(); i++) {
            if (i > 0) {
                result.add(", "); //$NON-NLS-1$
            }
            result.add(function.getParameters().get(i));
        }
        result.add(")"); //$NON-NLS-1$
        Literal intervalType = (Literal)function.getParameters().get(0);
        String interval = ((String)intervalType.getValue()).toUpperCase();
        String newInterval = INTERVAL_MAP.get(interval);
        if (newInterval != null) {
            intervalType.setValue(newInterval);
        } else if (supportsQuarter && interval.equals(NonReserved.SQL_TSI_QUARTER)) {
            intervalType.setValue("QUARTER"); //$NON-NLS-1$
        } else if (add) {
            if (interval.equals(NonReserved.SQL_TSI_FRAC_SECOND)) {
                intervalType.setValue("MILLISECOND"); //$NON-NLS-1$
                result.add(4, " / 1000000"); //$NON-NLS-1$
            } else if (interval.equals(NonReserved.SQL_TSI_QUARTER)) {
                intervalType.setValue(ExtractFunctionModifier.MONTH);
                result.add(4, " * 3"); //$NON-NLS-1$
            } else {
                intervalType.setValue(ExtractFunctionModifier.DAY);
                result.add(4, " * 7"); //$NON-NLS-1$
            }
        } else if (interval.equals(NonReserved.SQL_TSI_FRAC_SECOND)) {
            intervalType.setValue("MILLISECOND"); //$NON-NLS-1$
            result.add(" * 1000000"); //$NON-NLS-1$
        } else if (interval.equals(NonReserved.SQL_TSI_QUARTER)) {
            intervalType.setValue(ExtractFunctionModifier.MONTH);
            result.add(" / 3"); //$NON-NLS-1$
        } else {
            intervalType.setValue(ExtractFunctionModifier.DAY);
            result.add(" / 7"); //$NON-NLS-1$
        }
        if (!literalPart) {
            result.set(1, intervalType.getValue());
        }
        return result;
    }

}