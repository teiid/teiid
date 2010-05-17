package org.teiid.translator.xml;

import java.io.InputStream;
import java.sql.SQLException;

public interface Document {

	public InputStream getStream() throws SQLException;

	public String getCachekey();

}
