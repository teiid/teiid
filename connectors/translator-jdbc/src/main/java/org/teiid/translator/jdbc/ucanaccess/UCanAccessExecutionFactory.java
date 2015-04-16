package org.teiid.translator.jdbc.ucanaccess;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.hsql.HsqlExecutionFactory;

@Translator(name="ucanaccess", description="A translator for read/write Microsoft Access Database")
public class UCanAccessExecutionFactory extends HsqlExecutionFactory {
	
	public UCanAccessExecutionFactory() {
		setSupportsOrderBy(true);
		setMaxInCriteriaSize(JDBCExecutionFactory.DEFAULT_MAX_IN_CRITERIA);
		setMaxDependentInPredicates(10);
	}

	@Override
	public void start() throws TranslatorException {
		super.start();
		
		addPushDownFunction("ucanaccess", "DCount", TypeFacility.RUNTIME_NAMES.BIG_INTEGER, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING);
	}
	
	@Override
	public Object retrieveValue(ResultSet results, int columnIndex, Class<?> expectedType) throws SQLException {
		
		Integer code = DataTypeManager.getTypeCode(expectedType);
		
		if(code.intValue() == DataTypeManager.DefaultTypeCodes.BLOB) {
			
			byte[] buf = null;
			
			try {
				Blob blob = results.getBlob(columnIndex);
				InputStream in = blob.getBinaryStream();
				ByteArrayOutputStream buffer = new ByteArrayOutputStream();

				int nRead;
				byte[] data = new byte[1024 * 10];

				while ((nRead = in.read(data, 0, data.length)) != -1) {
				  buffer.write(data, 0, nRead);
				}

				buffer.flush();
				buf = buffer.toByteArray();
				in.close();				
			} catch (Exception e) {
				// ignore
			}
			
			try {
				buf = results.getBytes(columnIndex);
			} catch (SQLException e) {
				// ignore
			}
			
			if(buf == null) {
				return null;
			} else {
				final byte[] source = buf;
				return new BlobImpl(new InputStreamFactory () {
					
					@Override
					public InputStream getInputStream() throws IOException {
						return  new ByteArrayInputStream(source);
					}});
			}			
		}
		
		return super.retrieveValue(results, columnIndex, expectedType);
	}

	@Override
    public String getHibernateDialectClassName() {
		throw new IllegalStateException("UCanAccess Dialect not set"); //$NON-NLS-1$
    }
    
    @Override
    public boolean supportsDependentJoins() {
    	return false;
    }
}
