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
import java.util.Date;
import java.util.TimeZone;

import org.arrah.framework.analytics.PIIValidator;
import org.arrah.framework.datagen.EncryptRTM;
import org.arrah.framework.datagen.ShuffleRTM;
import org.arrah.framework.datagen.TimeUtil;
import org.arrah.framework.dataquality.FormatCheck;
import org.arrah.framework.profile.StatisticalAnalysis;
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
     * @param val - String that need to be encrpyted
     * @param key - key given to encrypt
     * @return String array after encryption
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static String encryptStr(String val, String key) {
        String[] results = new EncryptRTM().encryptStrArray(new String[]{val}, key);
        return (results != null && results.length == 1) ? results[0] : null;
    }
    
    /**
     * @param val - String that need to be decrpyted
     * @param key - key given to decrypt
     * @return String array after decryption
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static String decryptStr(String val, String key) {
        String[] results = new EncryptRTM().decryptStrArray(new String[]{val}, key);
        return (results != null && results.length == 1) ? results[0] : null;
    }
    
    /**
     * @param val - String array that need to be encrpyted
     * @param key - key given to encrypt
     * @return String array after encryption
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static String[] encryptStrArray(String[]val, String key) {
        return new EncryptRTM().encryptStrArray(val,key);
    }
    
    /**
     * @param val - String array that need to be decrpyted
     * @param key - key given to decrypt
     * @return String array after decryption
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static String[] decryptStrArray(String[]val, String key) {
        return new EncryptRTM().decryptStrArray(val,key);
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
    public static boolean isValidCreditCard(String cc) {
        return new PIIValidator().isCreditCard(cc);
    }
    
    /**
     * @param ssn number
     * @return boolean if matches ssn logic
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static boolean isValidSSN(String ssn) {
        return new PIIValidator().isSSN(ssn);
    }
    
    /**
     * @param phone number
     * @return boolean if matches phone  logic more than 8 character less than 12 character
     * can't start with 000
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static boolean isValidPhone(String phone) {
        return new PIIValidator().isPhone(phone);
    }
    
    /**
     * @param email
     * @return boolean if valid email
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static boolean isValidEmail(String email) {
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
    
    /**
     * @param date to be converted into long
     * @return long value of date since Epoch in default timezone
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static long dateToEpoch(Date date){
        return TimeUtil.dateIntoSecond(date);
    }
    
    /**
     * @param date to be converted into long
     * @param timezone - String TimeZone 
     * @return long value of date since Epoch
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static long dateToEpoch(Date date, String timezone){
        TimeZone tz = TimeZone.getTimeZone(timezone);
        return TimeUtil.dateIntoSecond(date, tz);
    }
    
    /**
     * @param millsec - time since epoch
     * @return Date of default timezone
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static Date epochToDate(long millsec) {
        return TimeUtil.secondIntoDate(millsec);
    }
    
    /**
     * @param millsec - time since epoch
     * @param timezone - String TimeZone 
     * @return Date of given timezone
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static Date epochToDate(long millsec, String timezone) {
        TimeZone tz = TimeZone.getTimeZone(timezone);
        return TimeUtil.secondIntoDate(millsec, tz);
    }
    
    /**
     * @param a - Date
     * @param b - Date
     * @return difference in milli seconds
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static long diffInSec(Date a, Date b) {
        return TimeUtil.diffIntoMilliSecond(a,b);
    }
    
    /**
     * @param date - Date
     * @param format - The format of date
     * @return formated date string
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static String convertToFormat (Date date, String format) {
        return FormatCheck.toFormatDate(date, format);
    }
    
    /**
     * @param numseries - series of numbers
     * @return standard deviation of the series
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static double stdDev (Number[] numseries){
        return new StatisticalAnalysis(numseries).getSDev() ;
    }
    
    /**
     * @param numseries - series of numbers
     * @return range maxiumn - minimum
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static double range (Number[] numseries) {
        return new StatisticalAnalysis(numseries).rangeObject() ;
    }
    
    /**
     * @param numseries
     * @return median or avg of the number series
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static double median (Number[] numseries) {
        return new StatisticalAnalysis(numseries).getMean() ;
    }
    
    /**
     * @param numseries
     * @return variance of the number series
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static double variance (Number[] numseries) {
        return new StatisticalAnalysis(numseries).getVariance() ;
    }
    
}
