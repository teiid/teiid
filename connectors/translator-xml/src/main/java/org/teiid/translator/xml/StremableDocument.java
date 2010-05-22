package org.teiid.translator.xml;

import java.io.InputStream;
import java.sql.SQLException;
import java.sql.SQLXML;

public class StremableDocument {

	private SQLXML xml;
	private String cacheKey;
	
	public StremableDocument(SQLXML xml, String cacheKey) {
		this.xml = xml;
		this.cacheKey = cacheKey;
	}
	
	public InputStream getStream() throws SQLException{
		return xml.getBinaryStream();
	}
	
	public String getCachekey() {
		return cacheKey;
	}

}
