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
 * BooksView_BooksSkeleton.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package metamatrix.com.Books;

import com.metamatrix.www.BooksView_Output.Books_Output_Type;

public class BooksViewBooks implements org.apache.axis.wsdl.Skeleton {
    private static java.util.Map _myOperations = new java.util.Hashtable();
    private static java.util.Collection _myOperationsList = new java.util.ArrayList();

    static {
        org.apache.axis.description.OperationDesc _oper;
        org.apache.axis.description.ParameterDesc [] _params;
        _params = new org.apache.axis.description.ParameterDesc [] {
            new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("http://www.metamatrix.com/BooksView_Input", "AuthorBooks_Input"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://www.metamatrix.com/BooksView_Input", "AuthorBooks_Input_Type"), com.metamatrix.www.BooksView_Input.AuthorBooks_Input_Type.class, false, false),  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        };
        _oper = new org.apache.axis.description.OperationDesc("getBooks", _params, new javax.xml.namespace.QName("http://www.metamatrix.com/BooksView_Output", "Books_Output")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        _oper.setReturnType(new javax.xml.namespace.QName("http://www.metamatrix.com/BooksView_Output", ">Books_Output")); //$NON-NLS-1$ //$NON-NLS-2$
        _oper.setElementQName(new javax.xml.namespace.QName("", "getBooks")); //$NON-NLS-1$ //$NON-NLS-2$
        _oper.setSoapAction("BooksView_WS.BooksView_Books.getBooks"); //$NON-NLS-1$
        _myOperationsList.add(_oper);
        if (_myOperations.get("getBooks") == null) { //$NON-NLS-1$
            _myOperations.put("getBooks", new java.util.ArrayList()); //$NON-NLS-1$
        }
        ((java.util.List)_myOperations.get("getBooks")).add(_oper); //$NON-NLS-1$        
    }
        
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

    
    /**
     * Shold send this output 
     * <BooksView_Output:Books_Output xmlns:BooksView_Output="http://www.metamatrix.com/BooksView_Output">
     * <Books_Output_Instance>
     *      <FIRSTNAME>Elfriede</FIRSTNAME>
     *      <LASTNAME>Dustin</LASTNAME>
     *      <TITLE>Automated Software Testing</TITLE>
     * </Books_Output_Instance>
     * </BooksView_Output:Books_Output>
     */    
    public com.metamatrix.www.BooksView_Output.Books_Output_Type[] getBooks(com.metamatrix.www.BooksView_Input.AuthorBooks_Input_Type booksView_Books_getBooks_AuthorBooks_InputMsg) throws java.rmi.RemoteException {
        Books_Output_Type out = new Books_Output_Type("Elfriede", "Dustin", "Automated Software Testing", 1); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        return new Books_Output_Type[] {out};
    }
}
