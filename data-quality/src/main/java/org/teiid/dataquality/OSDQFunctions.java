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

package org.teiid.dataquality;

import java.util.ArrayList;

import org.arrah.framework.analytics.PIIValidator;
import org.arrah.framework.datagen.ShuffleRTM;
import org.arrah.framework.util.StringCaseFormatUtil;
import org.simmetrics.metrics.CosineSimilarity;
import org.simmetrics.metrics.JaccardSimilarity;
import org.simmetrics.metrics.JaroWinkler;
import org.simmetrics.metrics.Levenshtein;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.function.TeiidFunction;
import org.teiid.query.function.metadata.FunctionCategoryConstants;

/**
 * This class will be reflectively loaded in engine, so be cautioned about
 * renaming this.
 */
public class OSDQFunctions {

    /**
     * @param sourceValue The string that need to randomize
     *   vivek singh' will become 'ihg vkeivh'
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS, determinism=Determinism.COMMAND_DETERMINISTIC)
    public static String random(String sourceValue) {
        return ShuffleRTM.shuffleString(sourceValue);
    }

    /**
     * @param sourceValue
     * @return This function will return digit characters of the string
     *
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static String digit(String sourceValue) {
        return StringCaseFormatUtil.digitString(sourceValue);
    }

    /**
     * @param val
     * @return -1 of no match otherwise index of the first match
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static int whitespaceIndex(String val) {
        return StringCaseFormatUtil.whitespaceIndex(val);
    }

    /**
     * @param cc Credit Card number
     * @return boolean if matches credit card logic and checksum
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static boolean validCreditCard(String cc) {
        return new PIIValidator().isCreditCard(cc);
    }

    /**
     * @param ssn number
     * @return boolean if matches ssn logic
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static boolean validSSN(String ssn) {
        return new PIIValidator().isSSN(ssn);
    }

    /**
     * @param phone number
     * @return boolean if matches phone  logic more than 8 character less than 12 character
     * can't start with 000
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static boolean validPhone(String phone) {
        return new PIIValidator().isPhone(phone);
    }

    /**
     * @param email
     * @return boolean if valid email
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static boolean validEmail(String email) {
        return new PIIValidator().isEmail(email);
    }

    /**
     * @return float distance
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static float cosineDistance(String a, String b) {
        ArrayList<Character> alist = StringCaseFormatUtil.toArrayListChar(a);
        ArrayList<Character> blist = StringCaseFormatUtil.toArrayListChar(b);
        java.util.Set<Character> aset = new java.util.HashSet<Character>(alist);
        java.util.Set<Character> bset = new java.util.HashSet<Character>(blist);
        return new CosineSimilarity<Character>().compare(aset, bset);
    }

    /**
     * @return float distance
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static float jaccardDistance(String a, String b) {
        ArrayList<Character> alist = StringCaseFormatUtil.toArrayListChar(a);
        ArrayList<Character> blist = StringCaseFormatUtil.toArrayListChar(b);
        java.util.Set<Character> aset = new java.util.HashSet<Character>(alist);
        java.util.Set<Character> bset = new java.util.HashSet<Character>(blist);
        return new JaccardSimilarity<Character>().compare(aset, bset);
    }

    /**
     * @return float distance
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static float jaroWinklerDistance(String a, String b) {
        return new JaroWinkler().compare(a, b);
    }

    /**
     * @return float distance
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static float levenshteinDistance(String a, String b) {
        return new Levenshtein().compare(a, b);
    }

}
