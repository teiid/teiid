/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

/*
 */
package com.metamatrix.connector.jdbc.util;

import java.util.Iterator;
import java.util.List;

import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.TypeFacility;
import com.metamatrix.connector.api.ValueTranslator;
import com.metamatrix.connector.basic.BasicValueTranslator;
import com.metamatrix.connector.exception.ConnectorException;

/**
 */
public class JDBCExecutionHelper {

    
    public static Object convertValue(Object value, Class expectedType, List valueTranslators, TypeFacility typeFacility, boolean trimStrings, ExecutionContext context) throws ConnectorException {
        if(expectedType.isAssignableFrom(value.getClass())){
            return value;
        }
        ValueTranslator translator = determineTransformation(value.getClass(), expectedType, valueTranslators, typeFacility);
        Object result = translator.translate(value, context);
        if(trimStrings && result instanceof String){
            result = trimString((String)result);
        }
        return result;
    }

    /**
     * @param actualType
     * @param expectedType
     * @return Transformation between actual and expected type
     */
    public static ValueTranslator determineTransformation(Class actualType, Class expectedType, List valueTranslators, TypeFacility typeFacility) throws ConnectorException {
        ValueTranslator valueTranslator = null;
        
        //check valueTranslators first
        if(valueTranslators != null && !valueTranslators.isEmpty()){        
            Iterator iter = valueTranslators.iterator();
            while(iter.hasNext()){
                ValueTranslator translator = (ValueTranslator)iter.next();
                
                //Evaluate expressions in this order for performance.
                if(expectedType.equals(translator.getTargetType()) && translator.getSourceType().isAssignableFrom(actualType)){
                    valueTranslator = translator;
                    break;
                }
            }
        }
        
        if(valueTranslator == null){
            valueTranslator = new BasicValueTranslator(actualType, expectedType, typeFacility);
        }
        return valueTranslator;
    }
    
    /**
     * Expects string to never be null 
     * @param value Incoming value
     * @return Right trimmed value  
     * @since 4.2
     */
    public static String trimString(String value) {
        for(int i=value.length()-1; i>=0; i--) {
            if(value.charAt(i) != ' ') {
                // end of trim, return what's left
                return value.substring(0, i+1);
            }
        }

        // All spaces, so trim it all
        return ""; //$NON-NLS-1$        
    }
        
}
