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

package com.metamatrix.connector.text;

import java.util.*;
import java.text.*;

import org.teiid.connector.api.ConnectorLogger;


/**
 * <p>This is a helper class for  TextTranslators that can be used to translate
 * any String that is a representation of a date to a java.util.Date object.
 * This class is created by passing it a properties object that contains at least
 * one property.  The value of this property is a String that can be a delimited list of 
 * date format Strings.  Date format Strings can be any format String that a 
 * java.text.SimpleDateFormat can be created with.  There is also an optional
 * property that defines the delimiter for the list of Date format Strings.
 * If this property is not specified then the entire String value of the DateFormatStrings
 * property will be used as the only format that this class can translate.</p>
 *
 * <p>After this class is created it can the be used to translate Strings into 
 * java.util.Date object using the translateStringToDate(String) method.</p>
 */
public class StringToDateTranslator {

    /**
    * The SimpleDateFormat objects that are used to translate dates for this
    * connector.
    */
    private List simpleDateFormats;

    /**
    * The List of date format Strings that define the patterns of the Strings
    * that this translator can translate.
    */
    private List dateFormatStrings;

    /**
    * This boolean determines whether this instance has any DateFormatters
    * to translate Strings with.  It is cached for performance reasons.
    */
    private boolean hasFormatters=false;

    private ConnectorLogger logger;
    
    /**
    * <p>This class is created by passing it a properties object that contains at least
    * one property.  The value of this property is a String that can be a delimited list of 
    * date format Strings.  Date format Strings can be any format String that a 
    * java.text.SimpleDateFormat can be created with.  There is also an optional
    * property that defines the delimiter for the list of Date format Strings.
    * If this property is not specified then the entire String value of the DateFormatStrings
    * property will be used as the only format that this class can translate.</p>
    *
    * <p>Note that the order of the list of date format strings is important
    * because the translator attempts to translate the String passed to it in the
    * order of the date format Strings in the delimited property value.</p>
    *
    * <p>After this class is created it can the be used to translate Strings into 
    * java.util.Date object using the translateStringToDate(String) method.</p>
    *
    * <pre>
    * The properties that are required in construction of this class are as 
    * follows:
    *
    * PropertyName: TextPropertyNames.DATE_RESULT_FORMATS
    * PropertyValue: Delimited list of date format Strings: ie MM/dd/yy' 'hh:mm:ss
    *
    * PropertyName: TextPropertyNames.DATE_RESULT_FORMATS_DELIMITER
    * PropertyValue: Delimiter for value of TextPropertyNames.DATE_RESULT_FORMATS
    * </pre>
    */
    public StringToDateTranslator(Properties props, ConnectorLogger logger) {

        if (props==null) {
            return;
        }
        
        this.logger = logger;
    
        String dateFormats = props.getProperty(TextPropertyNames.DATE_RESULT_FORMATS);
        String dateFormatsDelimiter = props.getProperty(TextPropertyNames.DATE_RESULT_FORMATS_DELIMITER);
        if (!(dateFormatsDelimiter == null || dateFormatsDelimiter.trim().length() == 0)) {
            if (!(dateFormats == null || dateFormats.trim().length() == 0)) {
                createSimpleDateFormats(dateFormats, dateFormatsDelimiter);
            }
        } else if (!(dateFormats == null || dateFormats.trim().length() == 0)) {
            createSimpleDateFormat(dateFormats);
        }
    
        if (simpleDateFormats != null && simpleDateFormats.size() > 0) {
            hasFormatters = true;
        }
    }

    /**
    * This method is used to translate String representations of dates into 
    * java.util.Date objects using a set of formats passed into this class at 
    * creation time.  Has formatters should always be called on this class
    * prior to the use of this method to determine whether or not there are any
    * formatters this class can use to translate the String value passed in.
    * If there are no formatters and this method is called, it will throw
    * a parse Exception.
    *
    * @param string the String to be parsed into a java.util.Date
    * @return the java.util.Date representation of the passed in String
    * @throws ParseException if the String passed in could not be parsed
    */
    public java.util.Date translateStringToDate(String string) throws ParseException{
        List parseExceptionList = new ArrayList();            
        Iterator iterator = simpleDateFormats.iterator();
        
        while (iterator.hasNext()) {
            SimpleDateFormat formatter = (SimpleDateFormat)iterator.next();
            try {
                java.util.Date date = formatter.parse(string);
                return date;
            }catch(ParseException e) {
                parseExceptionList.add(e);
                // Do nothing here will try again with the next formatter
            }
        }
        
        // if we have reached this point without returning a Date, we 
        // have been unsuccessful in parsing: throw an exception
        
        // This should always be the case, but just for safety:
        if (dateFormatStrings.size() == parseExceptionList.size()) {
            StringBuffer message = new StringBuffer();
            int counter = 0;
            Object[] params = new Object[] { string };
            message.append(TextPlugin.Util.getString("StringToDateTranslator.Attempts_to_parse_String__{0}_to_a_java.util.Date_failed_for_the_following_reasons___1", params)); //$NON-NLS-1$
            
            if (!hasFormatters()) {
                message.append(TextPlugin.Util.getString("StringToDateTranslator.There_is_no_format_Strings_found_in_this_formatter_object._n_2", params)); //$NON-NLS-1$
            }
            
            Iterator exceptionsIterator = parseExceptionList.iterator();
            Iterator formatStringsIterator = dateFormatStrings.iterator();
            while (exceptionsIterator.hasNext()) {
                String format = (String)formatStringsIterator.next();
                String exceptionMessage = ((ParseException)exceptionsIterator.next()).getMessage();
                Object[] params2 = new Object[] { ""+counter, format, exceptionMessage }; //$NON-NLS-1$
                message.append(TextPlugin.Util.getString("StringToDateTranslator.Parse_Attempt__{0}_using_format__{1}_failed_for_the_following_reason__{2}_4", params2)); //$NON-NLS-1$
                counter++;
            }
            
            throw new ParseException(message.toString(),0);
        }
        Object params3 = new Object[] { string, dateFormatStrings };
        throw new ParseException(TextPlugin.Util.getString("StringToDateTranslator.Failed_to_convert_String__{0}_to_a_Date_using_one_of_the_following_format_Strings_that_are_specified_in_the_properties_for_this_Connector__{1}_1", params3), 0); //$NON-NLS-1$
    }

    /**
    * This method is used to check the status of this translator object.
    * It will return true if this translator has any 'formatters' to do parsing
    * of Strings in the translateStringToDate() method.  This method should
    * always be called prior to using the translateStringToDate() method.
    * If there are no formatters for the instance of this class, all calls to 
    * the translateStringToDate() method will throw a ParseException.
    * 
    * @return true if this class has formatters to parse Strings to Dates
    */
    public boolean hasFormatters() {
        return hasFormatters;
    }

    /**
    * This method is a helper method that will instantiate the formatters that
    * this object uses to translate Strings into Dates.
    *
    * @param dateFormats the delimited String of date format templates to 
    * be used to create the date formatters for this object.
    * @param dateFormatsDelimiter the delimiter used to delimit the dateFormats
    * String that is also passed into this method.
    */
    private void createSimpleDateFormats(String dateFormats, String dateFormatsDelimiter) {
        simpleDateFormats = new ArrayList();
        dateFormatStrings = new ArrayList();
        StringTokenizer tokenizer = new StringTokenizer(dateFormats, dateFormatsDelimiter);
        
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            logger.logTrace("Creating simple Date format for formatting String: " +token); //$NON-NLS-1$
        
            dateFormatStrings.add(token);
            SimpleDateFormat formatter = new SimpleDateFormat(token.trim());
            simpleDateFormats.add(formatter);
        }
    }

    /**
    * This method is as helper method that will create a single date formatter
    * from the date format template String that is passed into it.
    *
    * @param dateFormats the String that is the template for translating
    * Strings into java.util.Date objects
    */
    private void createSimpleDateFormat(String dateFormats) {
        simpleDateFormats = new ArrayList();
        dateFormatStrings = new ArrayList();
        logger.logTrace("Creating simple Date format for formatting String: " +dateFormats); //$NON-NLS-1$
    
        SimpleDateFormat formatter = new SimpleDateFormat(dateFormats);
        dateFormatStrings.add(dateFormats);
        simpleDateFormats.add(formatter);
    }
      
}
