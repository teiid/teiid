package org.teiid.query.function;

import org.arrah.framework.datagen.ShuffleRTM;
import org.arrah.framework.ndtable.ResultsetToRTM;
import org.arrah.framework.util.StringCaseFormatUtil;

public class Maskutil {

    // ================= Function - Maskutil ========================
    
    /**
     * @param a
     *  The string that need to randomize
     *   vivek singh' will become 'ihg vkeivh'
     */
    public static Object toRandomValue(String sourceValue) {
        return (String)ShuffleRTM.shuffleString(sourceValue);
    }

    /**
     * @param a
     * This function will retrun MD5 hashcode of the string
     * @return String
     */
    public static String toHashValue(String sourceValue) {
        if (sourceValue == null)
            return "d41d8cd98f00b204e9800998ecf8427e"; // null MD5 value
        return ResultsetToRTM.getMD5(sourceValue).toString();
    }

    /**
     * @param a
     * This function will return digit characters of the string
     * @return
     * 
     */
    public static String toDigitValue(String sourceValue) {
        return StringCaseFormatUtil.digitString(sourceValue);
    }
    
    
}
