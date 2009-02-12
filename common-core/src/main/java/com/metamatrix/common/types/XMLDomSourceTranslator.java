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

package com.metamatrix.common.types;

import java.io.IOException;
import java.util.Properties;

import javax.xml.transform.Source;
import javax.xml.transform.dom.DOMSource;


/** 
 * This class converts the DOMSource input documents into other types of XML
 * documents.
 */
public class XMLDomSourceTranslator extends BaseXMLTranslator {

    DOMSource source;
    
    public XMLDomSourceTranslator(DOMSource source, Properties props) {
        super(props);
        this.source = source;
    }

    /** 
     * @see com.metamatrix.common.types.XMLTranslator#getSource()
     */
    public Source getSource() throws IOException {
        return source;
    }
}
