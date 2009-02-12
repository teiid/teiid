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



package com.metamatrix.connector.xml.base;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.commons.codec.binary.Base64;

import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.xml.DocumentProducer;
import com.metamatrix.connector.xml.cache.DocumentCache;
import com.metamatrix.connector.xml.cache.IDocumentCache;
import com.metamatrix.connector.xml.http.Messages;

// Response class

public class Response {
	private XMLDocument[] docs;

	private String[] cacheKeys;

	private String id;

	private DocumentProducer docProducer;

	IDocumentCache cache;

	private String cacheReference;

	public Response(XMLDocument[] docs, String[] cacheKeys,
			DocumentProducer docProducer, IDocumentCache cache, String cacheReference) {
		this.docs = docs;
		this.cacheKeys = cacheKeys;
		this.id = null;
		this.docProducer = docProducer;
		this.cache = cache;
		this.cacheReference = cacheReference;
	}

	public Response(String id, DocumentProducer docProducer,
			IDocumentCache cache, String cacheReference) {
		this.docs = null;
		this.cacheKeys = null;
		this.id = id;
		this.docProducer = docProducer;
		this.cache = cache;
		this.cacheReference = cacheReference;
	}

	public XMLDocument[] getDocuments() throws ConnectorException {
		if (docs == null) {
			getDocsFromCache();
		}
		return docs;
	}

	public String getResponseId() throws ConnectorException {
		if (id == null) {
			calculateResponseId();
		}
		return id;
	}

	private void getDocsFromCache() throws ConnectorException {
		if (id == null) {
			calculateResponseId();
		}
		int i = id.indexOf(',');
		String strDocCount = id.substring(0, i);
		int docCount = new Integer(strDocCount).intValue();
		docs = new XMLDocument[docCount];
		cacheKeys = new String[docCount];
		Serializable[] requests = null;
		for (int d = 0; d < docCount; d++) {
			int j = id.indexOf(',', i + 1);
			String strLen = id.substring(i + 1, j);
			int len = new Integer(strLen).intValue();
			String cacheKey = id.substring(j + 1, j + 1 + len);
			cacheKeys[d] = cacheKey;
			i = j + 1 + len;
			i = id.indexOf(',', i + 2);
		}
		for (int d = 0; d < docCount; d++) {
			// Before decoding the documents themselves, let's see if we can
			// find them in the cache
			XMLDocument doc = DocumentCache.cacheLookup(cache, cacheKeys[d],
					cacheReference);
			if (doc == null) {
				throw new ConnectorException("No Document Found");
			}
			docs[d] = doc;
		}
	}

	private void calculateResponseId() throws ConnectorException {
		StringBuffer buf = new StringBuffer();
		buf.append(docs.length);
		buf.append(',');
		Serializable[] requests = new Serializable[docs.length];
		for (int i = 0; i < docs.length; i++) {
			String cacheKey = cacheKeys[i];
			buf.append(cacheKey.length());
			buf.append(',');
			buf.append(cacheKey);
			buf.append(',');
			requests[i] = docProducer.getRequestObject(i);
		}
		try {
			String str = encodeAsString(requests);
			buf.append(str);
		} catch (IOException ioe) {
			ConnectorException ce = new ConnectorException(Messages
					.getString("Executor.unable.to.encode.response.id")); //$NON-NLS-1$
			ce.setStackTrace(ioe.getStackTrace());
			throw ce;
		}
		id = buf.toString();
	}

	private String encodeAsString(Serializable ser) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(ser);
		oos.flush();
		oos.close();
		baos.flush();
		baos.close();
		byte[] bytes = baos.toByteArray();
		Base64 encoder = new Base64();
		String str = new String(encoder.encode(bytes));
		return str;
	}

	private Serializable decodeFromString(String str) throws IOException,
			ClassNotFoundException {
		Base64 decoder = new Base64();
		byte[] bytes = decoder.decode(str.getBytes());
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		ObjectInputStream ois = new ObjectInputStream(bais);
		Serializable ser = (Serializable) ois.readObject();
		return ser;
	}
}
// End of Response class
