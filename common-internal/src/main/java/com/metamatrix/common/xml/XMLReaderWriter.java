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

package com.metamatrix.common.xml;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.jdom.Document;
import org.jdom.JDOMException;


/** 
* This interface is used to read and write JDOM compliant XML files.
*/
public interface XMLReaderWriter {

    /**
    * This method will write a JDOM Document to an OutputStream.
    *
    * @param doc the JDOM document to be written to the OutputStream
    * @param stream the output stream to be written to.
    * @throws IOException if there is a problem writing to the OutputStream
    */
    public void writeDocument(Document doc, OutputStream stream) throws IOException;
    
    /**
    * This method will write a JDOM Document to an OutputStream.
    *
    * @param stream the input stream to read the XML document from.
    * @return the JDOM document reference that represents the XML text in the
    * InputStream.
    * @throws IOException if there is a problem reading from the InputStream
    * @throws JDOMException if the InputStream does not represent a JDOM 
    * compliant XML document.
    */
    public Document readDocument(InputStream stream) throws JDOMException, IOException;
    
}
