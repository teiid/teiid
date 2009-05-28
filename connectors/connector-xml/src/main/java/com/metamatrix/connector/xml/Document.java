package com.metamatrix.connector.xml;

import java.io.InputStream;

public interface Document {

	public InputStream getStream();

	public String getCachekey();

}
