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

package com.metamatrix.query.function.source;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.SQLException;
import java.sql.SQLXML;

import net.sf.saxon.trans.XPathException;

import com.metamatrix.api.exception.query.FunctionExecutionException;
import com.metamatrix.internal.core.xml.XPathHelper;
import com.metamatrix.query.QueryPlugin;




/** 
 * This class contains scalar system functions supporting for XML manipulation.
 * 
 * @since 4.2
 */
public class XMLSystemFunctions {

    public static Object xpathValue(Object document, Object xpathStr) throws FunctionExecutionException {
        if(document == null || xpathStr == null) {
            return null;
        }
        
        Reader stream = null;
        
        if (document instanceof SQLXML) {
            try {
                stream = ((SQLXML)document).getCharacterStream();
            } catch (SQLException e) {
                throw new FunctionExecutionException(QueryPlugin.Util.getString("XMLSystemFunctions.xpathvalue_takes_only_string", document.getClass().getName())); //$NON-NLS-1$
            }
        } else if(document instanceof String) {
            stream = new StringReader((String)document);
        } else {
            throw new FunctionExecutionException(QueryPlugin.Util.getString("XMLSystemFunctions.xpathvalue_takes_only_string", document.getClass().getName())); //$NON-NLS-1$
        }
        
        try {
            return XPathHelper.getSingleMatchAsString(stream, (String) xpathStr);
        } catch(IOException e) {
            throw new FunctionExecutionException(QueryPlugin.Util.getString("XMLSystemFunctions.wrap_exception", xpathStr, e.getMessage())); //$NON-NLS-1$
        } catch(XPathException e) {
            throw new FunctionExecutionException(QueryPlugin.Util.getString("XMLSystemFunctions.wrap_exception", xpathStr, e.getMessage())); //$NON-NLS-1$
        }
    }
    
}
