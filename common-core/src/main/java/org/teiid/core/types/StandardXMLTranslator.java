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

package org.teiid.core.types;

import java.io.IOException;
import java.io.Writer;

import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;

public class StandardXMLTranslator extends XMLTranslator {
	
	private static ThreadLocal<TransformerFactory> threadLocalTransformerFactory = new ThreadLocal<TransformerFactory>() {
		protected TransformerFactory initialValue() {
			return TransformerFactory.newInstance();
		}
	};
	
	public static TransformerFactory getThreadLocalTransformerFactory() {
		return threadLocalTransformerFactory.get();
	}
    
    private Source source;
        
    public StandardXMLTranslator(Source source) {
    	this.source = source;
    }
    
    @Override
    public void translate(Writer writer) throws TransformerException, IOException {
        Transformer t = threadLocalTransformerFactory.get().newTransformer();
        t.transform(source, new StreamResult(writer));
    }
        
}
