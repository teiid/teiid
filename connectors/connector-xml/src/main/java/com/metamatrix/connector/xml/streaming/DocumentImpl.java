package com.metamatrix.connector.xml.streaming;

import java.io.InputStream;
import java.sql.SQLException;
import java.sql.SQLXML;

public class DocumentImpl implements com.metamatrix.connector.xml.Document {

	private SQLXML xml;
	private String cacheKey;
	
	public DocumentImpl(SQLXML xml, String cacheKey) {
		this.xml = xml;
		this.cacheKey = cacheKey;
	}
	
	@Override
	public InputStream getStream() throws SQLException{
		return xml.getBinaryStream();
	}
	
	@Override
	public String getCachekey() {
		return cacheKey;
	}

}
