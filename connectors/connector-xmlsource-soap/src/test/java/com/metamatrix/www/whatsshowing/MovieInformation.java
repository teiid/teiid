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

/**
 * MovieInformationSoapSkeleton.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package com.metamatrix.www.whatsshowing;

public class MovieInformation {
    
    private static java.util.Map _myOperations = new java.util.Hashtable();
    private static java.util.Collection _myOperationsList = new java.util.ArrayList();

    /**
    * Returns List of OperationDesc objects with this name
    */
    public static java.util.List getOperationDescByName(java.lang.String methodName) {
        return (java.util.List)_myOperations.get(methodName);
    }

    /**
    * Returns Collection of OperationDescs
    */
    public static java.util.Collection getOperationDescs() {
        return _myOperationsList;
    }

    static {
        org.apache.axis.description.OperationDesc _oper;
        org.apache.axis.description.ParameterDesc [] _params;
        _params = new org.apache.axis.description.ParameterDesc [] {
            new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("http://www.metamatrix.com/whatsshowing", "zipCode"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"), java.lang.String.class, false, false),  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("http://www.metamatrix.com/whatsshowing", "radius"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"), int.class, false, false),  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        };
        _oper = new org.apache.axis.description.OperationDesc("getTheatersAndMovies", _params, new javax.xml.namespace.QName("http://www.metamatrix.com/whatsshowing", "GetTheatersAndMoviesResult")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        _oper.setReturnType(new javax.xml.namespace.QName("http://www.metamatrix.com/whatsshowing", "ArrayOfTheater")); //$NON-NLS-1$ //$NON-NLS-2$
        _oper.setElementQName(new javax.xml.namespace.QName("http://www.metamatrix.com/whatsshowing", "GetTheatersAndMovies")); //$NON-NLS-1$ //$NON-NLS-2$
        _oper.setSoapAction("http://www.metamatrix.com/whatsshowing/GetTheatersAndMovies"); //$NON-NLS-1$
        _myOperationsList.add(_oper);
        if (_myOperations.get("getTheatersAndMovies") == null) { //$NON-NLS-1$
            _myOperations.put("getTheatersAndMovies", new java.util.ArrayList()); //$NON-NLS-1$
        }
        ((java.util.List)_myOperations.get("getTheatersAndMovies")).add(_oper); //$NON-NLS-1$
        _params = new org.apache.axis.description.ParameterDesc [] {
            new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("http://www.metamatrix.com/whatsshowing", "month"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"), int.class, false, false),  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("http://www.metamatrix.com/whatsshowing", "year"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"), int.class, false, false),  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        };
        _oper = new org.apache.axis.description.OperationDesc("getUpcomingMovies", _params, new javax.xml.namespace.QName("http://www.metamatrix.com/whatsshowing", "GetUpcomingMoviesResult")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        _oper.setReturnType(new javax.xml.namespace.QName("http://www.metamatrix.com/whatsshowing", "ArrayOfUpcomingMovie")); //$NON-NLS-1$ //$NON-NLS-2$
        _oper.setElementQName(new javax.xml.namespace.QName("http://www.metamatrix.com/whatsshowing", "GetUpcomingMovies")); //$NON-NLS-1$ //$NON-NLS-2$
        _oper.setSoapAction("http://www.metamatrix.com/whatsshowing/GetUpcomingMovies"); //$NON-NLS-1$
        _myOperationsList.add(_oper);
        if (_myOperations.get("getUpcomingMovies") == null) { //$NON-NLS-1$
            _myOperations.put("getUpcomingMovies", new java.util.ArrayList()); //$NON-NLS-1$
        }
        ((java.util.List)_myOperations.get("getUpcomingMovies")).add(_oper); //$NON-NLS-1$
    }

    /*
     *       <Theater>
               <Name>AMC Chesterfield 14</Name>
               <Address>3rd Floor Chesterfield Mall, Chesterfield, MO</Address>
               <Movies>
                  <Movie>
                     <Rating>PG</Rating>
                     <Name>Barnyard: The Original Party Animals</Name>
                     <RunningTime>1 hr 30 mins</RunningTime>
                     <ShowTimes>12:10pm | 2:25pm | 4:45pm | 7:10pm | 9:30pm</ShowTimes>
                  </Movie>
                  <Movie>
                     <Rating>G</Rating>
                     <Name>Cars</Name>
                     <RunningTime>1 hr 57 mins</RunningTime>
                     <ShowTimes>12:00pm | 2:40pm</ShowTimes>
                  </Movie>
               </Theater>

     */
    
    public com.metamatrix.www.whatsshowing.Theater[] getTheatersAndMovies(java.lang.String zipCode, int radius) throws java.rmi.RemoteException
    {
        if (!zipCode.equals("63011")){ //$NON-NLS-1$
            throw new RuntimeException("WRONG zip"); //$NON-NLS-1$
        }
        
        Movie one = new Movie("PG", "Barnyard: The Original Party Animals", "1 hr 30 mins", "12:10pm | 2:25pm | 4:45pm | 7:10pm | 9:30pm"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        Movie two = new Movie("G", "Cars", "1 hr 30 mins", "1 hr 57 mins"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        Theater t1 = new Theater("AMC Chesterfield 14", "3rd Floor Chesterfield Mall, Chesterfield, MO", new Movie[] {one, two}); //$NON-NLS-1$ //$NON-NLS-2$
        com.metamatrix.www.whatsshowing.Theater[] ret = new Theater[] {t1};
        return ret;
    }

    public com.metamatrix.www.whatsshowing.UpcomingMovie[] getUpcomingMovies(int month, int year) throws java.rmi.RemoteException
    {
        com.metamatrix.www.whatsshowing.UpcomingMovie[] ret = null;
        return ret;
    }

}
