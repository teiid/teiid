package com.metamatrix.connector.xml.streaming;

import java.io.InputStream;

public class DocumentImpl implements com.metamatrix.connector.xml.Document {

	private InputStream xml;
	private String cacheKey;
	
	public DocumentImpl(InputStream stream, String cacheKey) {
		this.xml = stream;
		this.cacheKey = cacheKey;
	}
	
	public InputStream getStream() {
		return xml;
	}
	
	public String getCachekey() {
		return cacheKey;
	}

}
