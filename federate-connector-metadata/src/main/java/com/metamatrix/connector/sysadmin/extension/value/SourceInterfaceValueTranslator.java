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
package com.metamatrix.connector.sysadmin.extension.value;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.connector.sysadmin.SysAdminMethodManager;
import com.metamatrix.connector.sysadmin.extension.IObjectCommand;
import com.metamatrix.connector.sysadmin.extension.IValueTranslator;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.exception.ConnectorException;

/**
 */
public class SourceInterfaceValueTranslator implements IValueTranslator {
    static final String METHOD_GETPROPERTIES = "Properties"; //$NON-NLS-1$
//  static final String METHOD_GETPROPERY = "Property"; //$NON-NLS-1$
  static final String DELIM = "."; //$NON-NLS-1$
  
    private static Map usedMethods = new HashMap();

    private Class sourceClass = null;
    private Class targetClass = null;
    
    
//    private ConnectorEnvironment environment;
    
    public SourceInterfaceValueTranslator(Class sourceInterfaceClass) {
        this.sourceClass = sourceInterfaceClass;
        this.targetClass = sourceInterfaceClass;

    }
    
    public SourceInterfaceValueTranslator(Class sourceInterfaceClass, Class targetInterfaceClass) {
        this.sourceClass = sourceInterfaceClass;
        this.targetClass = targetInterfaceClass;

    }    
    /* (non-Javadoc)
     * @see com.metamatrix.connector.jdbc.extension.ValueTranslator#initialize(com.metamatrix.data.ConnectorEnvironment)
     */
    public void initialize(ConnectorEnvironment env) {
//        this.environment = env;     
    }
    
    public Class getSourceType() {
        return sourceClass;
    }

    public Class getTargetType() {
        return targetClass;
    }

    public Object translate(Object object, IObjectCommand command, ExecutionContext context) throws ConnectorException {
       
        LinkedList row = new LinkedList();
        
        Method m = null;
        String[] resultColumnNames = command.getResultColumnNames();
        String[] resultNameInSource = command.getResultNamesInSource();
        int s = resultColumnNames.length;
        for (int i=0; i<s; i++) {

            String methodName = null;
            if (resultNameInSource[i] != null) {
                methodName = resultNameInSource[i];
            } else {
                methodName = resultColumnNames[i];
            }
            
            if (methodName.indexOf(DELIM) > 0 ) {
              // special processing for obtaining a property from a PRoperties object
              // the nameInSource in the model should have it specified using either
              // of the 2 following methods:
              // 1.  Properties.<propertyname>
              // 2.  Property.<propertyname>  
                List tokens = StringUtil.getTokens(methodName, DELIM);                           
                          
                if (tokens.size() == 1) {
                    row.add("InvalidNameInSource:" + methodName);//$NON-NLS-1$
                    continue;                  
                }
                
                String methodCall = (String) tokens.get(0);
                String methodParm = (String) tokens.get(1);
                
                if (methodCall.equalsIgnoreCase(METHOD_GETPROPERTIES)) {
                    
                    m = getObjectMethod(object, methodCall, null);
                    
                    Properties props = (Properties) executeMethod(m, object, Collections.EMPTY_LIST);
                   
                    String value = props.getProperty(methodParm);
                    row.add(value);
                } else {
                    List args = new ArrayList(1);
                    args.add(String.class);
                    
                    m = getObjectMethod(object, methodCall, args );
                    
                    List parms = new ArrayList(1);
                    parms.add(methodParm);
                    String value = (String) executeMethod(m, object, parms);
                    row.add(value);
                    
                }
                
               
            } else {
            
                String getMethodName = "get" + methodName;//$NON-NLS-1$
                m = getObjectMethod(object, getMethodName, null);
                if (m == null) {
                    m = getObjectMethod(object, methodName, null);
                }
                
                if (m!=null) {
                
                    Object result = executeMethod(m, object, Collections.EMPTY_LIST);
                    row.add(result);
                } else {
                    row.add(object.toString());
                }
            }          
            
            

            
        }
        return row;
      }    
        
      
      protected Object executeMethod(Method m, Object api, List parms) throws ConnectorException {

          try {
              return m.invoke(api, parms.toArray());
   
          } catch(Exception ite) {
              throw new ConnectorException(ite);
          }
 
      }
      
      protected Method getObjectMethod(Object object, String methodName, List parms) throws ConnectorException {
          String key = object.getClass().getName() + "_" + methodName;//$NON-NLS-1$
          
        try {
            // because all methods are getters with no parameters, its made easier to use this cache
            if (usedMethods.containsKey(key)) {
                Method m = (Method) usedMethods.get(key);
                return m;
            }
              Method m = SysAdminMethodManager.getMethodFromObject(object.getClass(), methodName, parms);
              usedMethods.put(key, m);
              return m;
        } catch (java.lang.NoSuchMethodException nsm) {
            // there is not method
            usedMethods.put(key, null);
            return null;
        } catch (Exception err) {
            throw new ConnectorException(err);
        }
          
      }
              

}
