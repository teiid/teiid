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

package com.metamatrix.connector.object.extension.value;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.teiid.connector.api.ConnectorException;

import com.metamatrix.connector.object.ObjectSourceMethodManager;
import com.metamatrix.connector.object.extension.IObjectCommand;
import com.metamatrix.connector.object.extension.IValueRetriever;
import com.metamatrix.core.util.StringUtil;

/**
 * Retrieve objects by using getObject() for everything.  
 */
public class BasicValueRetriever implements IValueRetriever {
  
    static final String METHOD_GETPROPERTIES = "Properties"; //$NON-NLS-1$
//  static final String METHOD_GETPROPERY = "Property"; //$NON-NLS-1$
    static final String DELIM = "."; //$NON-NLS-1$
    
    private IObjectCommand objectCommand;
    private Map usedMethods = null;

    
    public BasicValueRetriever() {
    }
      
    public List convertIntoRow(Object object, IObjectCommand command) throws Exception {
        this.objectCommand = command;
        
            if (object == null) {
                return Collections.EMPTY_LIST;
            }
            if (object.getClass().isArray()) {
                List resultRows = Arrays.asList((Object[]) object);
                return resultRows;
                
            } else if (object instanceof Collection) {
                
                Collection resultRows = (Collection) object;
                return convertList(resultRows);

            }
            return convertObject(object);
                        
     }      


    private List convertList(Collection list) throws ConnectorException {
        if (list.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        
        LinkedList row = new LinkedList();
        row.addAll(list);
        

        return row;
    }
        
   private List convertObject(Object object) throws ConnectorException {
        LinkedList row = new LinkedList();
        
        Method m = null;
        String[] resultColumnNames = objectCommand.getResultColumnNames();
        String[] resultColomnNameInSource = objectCommand.getResultNamesInSource();
        int s = resultColumnNames.length;
        for (int i=0; i<s; i++) {
            if (resultColomnNameInSource[i] != null) {
                String methodName = resultColomnNameInSource[i];
            
                // if the method name has a DELIM in the name, then parse to get first
                // token as the method name
                if (methodName.indexOf(DELIM) > 0 ) {
    //                            (methodName.startsWith(METHOD_GETPROPERY) ||
    //                             methodName.startsWith(METHOD_GETPROPERTIES))  ) {
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
                                    
                }  else {
                    
                    m = getObjectMethod(object, methodName, null);
                    
                    if (m!=null) {
                    
                        Object result = executeMethod(m, object, Collections.EMPTY_LIST);
                        row.add(result);
                    } else {
                        row.add(object.toString());
                    }
                    
                }
                
            } else {
                // no NameInSource given, so 
                // first: use the column name to create getter method name
                // second: if getter not found, then try the column name as the method name
                // third: else do a toString on the object
                
                String methodName = resultColumnNames[i];

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
        } catch (IllegalArgumentException err) {
            throw new ConnectorException(err);
        } catch (IllegalAccessException err) {
            throw new ConnectorException(err);
        } catch (InvocationTargetException err) {
            throw new ConnectorException(err);
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
            Method m = ObjectSourceMethodManager.getMethodFromObject(getClass(), methodName, parms);
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
