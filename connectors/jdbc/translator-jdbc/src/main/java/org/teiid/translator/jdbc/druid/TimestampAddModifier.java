package org.teiid.translator.jdbc.druid;

import org.teiid.language.Function;
import org.teiid.language.Literal;
import org.teiid.language.SQLConstants;
import org.teiid.translator.jdbc.ExtractFunctionModifier;
import org.teiid.translator.jdbc.FunctionModifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Modification of timestamp class for Apache Druid.
 * Based on OracleTimestampModifier
 * Created by Don Krapohl 04/02/2021
 */
public class TimestampAddModifier extends FunctionModifier {
    private static Map<String, String> INTERVAL_MAP = new HashMap<String, String>();

    static {
        INTERVAL_MAP.put(SQLConstants.NonReserved.SQL_TSI_DAY, ExtractFunctionModifier.DAY);
        INTERVAL_MAP.put(SQLConstants.NonReserved.SQL_TSI_HOUR, ExtractFunctionModifier.HOUR);
        INTERVAL_MAP.put(SQLConstants.NonReserved.SQL_TSI_MINUTE, ExtractFunctionModifier.MINUTE);
        INTERVAL_MAP.put(SQLConstants.NonReserved.SQL_TSI_MONTH, ExtractFunctionModifier.MONTH);
        INTERVAL_MAP.put(SQLConstants.NonReserved.SQL_TSI_SECOND, ExtractFunctionModifier.SECOND);
        INTERVAL_MAP.put(SQLConstants.NonReserved.SQL_TSI_YEAR, ExtractFunctionModifier.YEAR);
    }

    @Override
    public List<?> translate(Function function) {
        ArrayList<Object> result = new ArrayList<Object>();
        result.add(function.getParameters().get(2));
        result.add(" + (INTERVAL '"); //$NON-NLS-1$
        Literal intervalType = (Literal) function.getParameters().get(0);
        String interval = ((String) intervalType.getValue()).toUpperCase();
        String newInterval = INTERVAL_MAP.get(interval);
        //by capabilities this must be a literal integer
        int value = (Integer) ((Literal) function.getParameters().get(1)).getValue();
        long adjustedValue = value;
        if (newInterval != null) {
            result.add(value);
        } else if (interval.equals(SQLConstants.NonReserved.SQL_TSI_QUARTER)) {
            newInterval = ExtractFunctionModifier.MONTH;
            adjustedValue = value * 3L;
            result.add(adjustedValue);
        } else if (interval.equals(SQLConstants.NonReserved.SQL_TSI_FRAC_SECOND)) {
            newInterval = ExtractFunctionModifier.SECOND;
            result.add(String.format("0.%09d", value)); //$NON-NLS-1$
            adjustedValue = 1;
        } else {
            newInterval = ExtractFunctionModifier.DAY;
            adjustedValue = value * 7L;
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
