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

package com.metamatrix.connector.sysadmin;

import java.lang.reflect.Method;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.metamatrix.core.CoreConstants;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.query.unittest.QueryMetadataInterfaceBuilder;


/** 
 * @since 4.3
 */
public class FakeObjectUtil {
    
    public static final String METHOD1="1"; //$NON-NLS-1$
    public static final String METHOD21="21"; //$NON-NLS-1$
    public static final String METHOD22="22"; //$NON-NLS-1$
    public static final String METHOD3="3"; //$NON-NLS-1$
    public static final String METHOD4="4"; //$NON-NLS-1$
    public static final String METHOD5="5"; //$NON-NLS-1$
    public static final String METHOD6="6"; //$NON-NLS-1$
    public static final String METHOD7="7"; //$NON-NLS-1$
    public static final String METHOD8="8"; //$NON-NLS-1$
    public static final String METHOD9="9"; //$NON-NLS-1$
       
    public static List TESTMETHODS = new ArrayList();
    

    public static final String MODEL_NAME = CoreConstants.SYSTEM_ADMIN_PHYSICAL_MODEL_NAME; 
    
    private static final String METHOD1_NAME="getHosts"; //$NON-NLS-1$
    private static final String METHOD2_NAME="getConnectorBindings"; //$NON-NLS-1$
    private static final String METHOD4_NAME="getUsers"; //$NON-NLS-1$
    private static final String METHOD5_NAME="getCaches"; //$NON-NLS-1$
    private static final String METHOD6_NAME="getGroups"; //$NON-NLS-1$
    private static final String METHOD7_NAME="fakeMethod"; //$NON-NLS-1$
    
    
    private static final String AMETHOD1=MODEL_NAME + "." + METHOD1_NAME; //$NON-NLS-1$
    private static final String AMETHOD2=MODEL_NAME + "." + METHOD2_NAME; //$NON-NLS-1$
    private static final String AMETHOD4=MODEL_NAME + "." + METHOD4_NAME; //$NON-NLS-1$
    private static final String AMETHOD5=MODEL_NAME + "." + METHOD5_NAME; //$NON-NLS-1$
    private static final String AMETHOD6=MODEL_NAME + "." + METHOD6_NAME; //$NON-NLS-1$
    private static final String AMETHOD7=MODEL_NAME + "." + METHOD7_NAME; //$NON-NLS-1$
    
    
    public static final String QUERY_METHOD1="exec " + AMETHOD1 + "()"; //$NON-NLS-1$ //$NON-NLS-2$
    public static final String QUERY_METHOD21="exec " + AMETHOD2 + "('*')"; //$NON-NLS-1$ //$NON-NLS-2$
    public static final String QUERY_METHOD22="exec " + AMETHOD2 + "('*', 1)"; //$NON-NLS-1$ //$NON-NLS-2$
    public static final String QUERY_METHOD4="exec " +  AMETHOD4 + "()";  //$NON-NLS-1$ //$NON-NLS-2$
    public static final String QUERY_METHOD5="exec " +  AMETHOD5 + "(1)"; //$NON-NLS-1$ //$NON-NLS-2$
    public static final String QUERY_METHOD6="exec " +  AMETHOD6 + "(1, 1)"; //$NON-NLS-1$ //$NON-NLS-2$
    public static final String QUERY_METHOD7="exec " +  AMETHOD7 + "(1, 1)"; //$NON-NLS-1$ //$NON-NLS-2$

   
    
    
    static {
        TESTMETHODS.add(METHOD1_NAME);
        TESTMETHODS.add(METHOD2_NAME);
        TESTMETHODS.add(METHOD4_NAME);
        TESTMETHODS.add(METHOD5_NAME);
        TESTMETHODS.add(METHOD6_NAME);
        TESTMETHODS.add(METHOD7_NAME);
    }
    
    public static boolean isValidMethod(Method m) {
        if (TESTMETHODS.contains(m.getName())) {
            
            if (m.getName().equalsIgnoreCase(AMETHOD2)) {
                if (m.getReturnType().getName().equals(Collection.class.getName())) {
                    return true;
                }
                return true;
            } else if (m.getName().equalsIgnoreCase(AMETHOD4) ) {
                  if (m.getReturnType().getName().equals(Collection.class.getName()) ) {
                      return true;
                  }
                  return false;
            } else if (m.getName().equalsIgnoreCase(AMETHOD5) ) {
                   if (m.getReturnType().getName().equals(FakeObject.class.getName()) ) {
                       return true;
                   }
                   return false;
            
            } else if (m.getName().equalsIgnoreCase(AMETHOD6) ) {
                if (m.getReturnType().getName().equals(Collection.class.getName()) ) {
                    return true;
                }
                return false;
         
             } 
            return true;
          
        }
        
        
        return false;
    }
    
    public static QueryMetadataInterfaceBuilder createBuilder(String method) {
        QueryMetadataInterfaceBuilder builder = new QueryMetadataInterfaceBuilder();               
        builder.addPhysicalModel(MODEL_NAME); 
        
        if (method.equalsIgnoreCase(METHOD1)) {
            createMethod1(builder);
        } else if (method.equalsIgnoreCase(METHOD21)) {
            createMethod21(builder);
        } else if (method.equalsIgnoreCase(METHOD22)) {
            createMethod22(builder);
        } else if (method.equalsIgnoreCase(METHOD4)) {
            createMethod4(builder);
        } else if (method.equalsIgnoreCase(METHOD5)) {
            createMethod5(builder);
        } else if (method.equalsIgnoreCase(METHOD6)) {
                createMethod6(builder); 
        } else if (method.equalsIgnoreCase(METHOD7)) {
            createMethod7(builder);            
        } else {
            throw new MetaMatrixRuntimeException("Invalid Method " + method); //$NON-NLS-1$
        }
        
        return builder;
        
//        builder.addInputParameter("getparam1", String.class); //$NON-NLS-1$
//        builder.addInputParameter("getparam2", String.class); //$NON-NLS-1$
//        builder.addInputParameter("getparam3", Integer.class);//$NON-NLS-1$
//        String [] columns = {"col1", "col2", "col3"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
//        Class[] types =  {String.class, Integer.class, String.class};
//        builder.addResultSet("resultSet", columns, types); //$NON-NLS-1$
//        builder.addProcedure("proc", "procNameInSource"); //$NON-NLS-1$ //$NON-NLS-2$
        
    }
    
    private static void createMethod1(QueryMetadataInterfaceBuilder builder) {
        builder.addProcedure(AMETHOD1, METHOD1_NAME); 
    }
    
    private static void createMethod21(QueryMetadataInterfaceBuilder builder) {
        builder.addInputParameter("arg", String.class); //$NON-NLS-1$
        
        builder.addProcedure(AMETHOD2, METHOD2_NAME); 
        
    }    
    
    private static void createMethod22(QueryMetadataInterfaceBuilder builder) {
        builder.addInputParameter("arg1", String.class); //$NON-NLS-1$
        builder.addInputParameter("arg2", Integer.class);//$NON-NLS-1$
          
        String [] columns = {"col1", "col2", "col3"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Class[] types =  {String.class, Integer.class, String.class};
        builder.addResultSet("resultSet", columns, types); //$NON-NLS-1$
        
        builder.addProcedure(AMETHOD2, METHOD2_NAME); 
        
    }     
        
    private static void createMethod4(QueryMetadataInterfaceBuilder builder) {
         
        String [] columns = {"col1", "col2", "col3"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Class[] types =  {String.class, Integer.class, String.class};
        builder.addResultSet("resultSet", columns, types); //$NON-NLS-1$

        builder.addProcedure(AMETHOD4, METHOD4_NAME); 

    }     
    
    private static void createMethod5(QueryMetadataInterfaceBuilder builder) {
        builder.addInputParameter("arg", Integer.class);//$NON-NLS-1$        
        
//        String [] columns = {"Name, StringValue, Property.pv"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
//        Class[] types =  {String.class, String.class, String.class};
        String [] columns = {"cacheDate"}; //$NON-NLS-1$ 
        Class[] types =  {Date.class};
        builder.addResultSet("resultSet", columns, types); //$NON-NLS-1$
        
        builder.addProcedure(AMETHOD5, METHOD5_NAME); 
        
    }  
    
    private static void createMethod6(QueryMetadataInterfaceBuilder builder) {
        builder.addInputParameter("arg1", Integer.class); //$NON-NLS-1$
        builder.addInputParameter("arg2", Integer.class);//$NON-NLS-1$
          
        String [] columns = {"Name, StringValue, Property.pv"}; //$NON-NLS-1$ 
        Class[] types =  {String.class, String.class, String.class};
        builder.addResultSet("resultSet", columns, types); //$NON-NLS-1$
        
        builder.addProcedure(AMETHOD6, METHOD6_NAME); 
        
    }   
    
    private static void createMethod7(QueryMetadataInterfaceBuilder builder) {
        builder.addInputParameter("arg1", Integer.class); //$NON-NLS-1$
        builder.addInputParameter("arg2", Integer.class);//$NON-NLS-1$
          
        String [] columns = {"Name, StringValue, Property.pv"}; //$NON-NLS-1$ 
        Class[] types =  {String.class, String.class, String.class};
        builder.addResultSet("resultSet", columns, types); //$NON-NLS-1$
        
        builder.addProcedure(AMETHOD7, METHOD7_NAME);
        
    }    
    
    public static boolean isResultEqual(String method, Object result) {
        
        if (method.equalsIgnoreCase(METHOD1)) {
            if (result == null) {
                return true;
            }            
        } else if (method.equalsIgnoreCase(METHOD21)) {
            if (result == null) {
                return true;
            }            
        } else if (method.equalsIgnoreCase(METHOD22)) {
            if (result == null) {
                return false;
            }            
            if (result instanceof Collection) {
              Collection c = (Collection) result;
              if (c.size() != 3) {
                  return false;
              }
            } 
            
        } else if (method.equalsIgnoreCase(METHOD3)) {
            if (result == null) {
                return true;
            }            
        } else if (method.equalsIgnoreCase(METHOD4)) {
            if (result == null) {
                return false;
            }            
            if (result instanceof Collection) {
              Collection c = (Collection) result;
              if (c.size() != 3) {
                  return false;
              }
            }
        } else if (method.equalsIgnoreCase(METHOD5)) {
            if (result == null) {
                return false;
            } 
            if (result instanceof FakeObject) {
                return true;
            }
        } else {
            throw new MetaMatrixRuntimeException("Invalid Method " + method); //$NON-NLS-1$
        }
        
        return false;
    }
    
    public static MethodTests getInstance(final String userName, final String sessionID) {
        
        MethodTests mt = new MethodTests();
        return mt;
        

    }
    
    public static MethodTests getInstanceBasedOnNoRole(final String userName, final String sessionID) {
        
        MethodTests mt = new MethodTests();
        return mt;
        

    }    
    
 
}
