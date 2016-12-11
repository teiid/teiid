/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
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
     * @param a
     *  The string that need to randomize
     *   vivek singh' will become 'ihg vkeivh'
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS, determinism=Determinism.COMMAND_DETERMINISTIC)
    public static String random(String sourceValue) {
        return ShuffleRTM.shuffleString(sourceValue);
    }

    /**
     * @param a
     * This function will return digit characters of the string
     * @return
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
     * @param Credit Card number
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
     * @param String a
     * @param String b
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
     * @param String a
     * @param String b
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
     * @param String a
     * @param String b
     * @return float distance
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static float jaroWinklerDistance(String a, String b) {
        return new JaroWinkler().compare(a, b);
    }
    
    /**
     * @param String a
     * @param String b
     * @return float distance
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static float levenshteinDistance(String a, String b) {
        return new Levenshtein().compare(a, b);
    }
    
}
