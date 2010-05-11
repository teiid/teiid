package org.teiid.core.types;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Reader;
import java.sql.SQLException;

import org.teiid.core.types.InputStreamFactory.StreamFactoryReference;


public class BaseLob implements Externalizable, StreamFactoryReference {
	
	private static final long serialVersionUID = -1586959324208959519L;
	private InputStreamFactory streamFactory;
	
	public BaseLob() {
		
	}
	
	protected BaseLob(InputStreamFactory streamFactory) {
		this.streamFactory = streamFactory;
	}
	
	public void setStreamFactory(InputStreamFactory streamFactory) {
		this.streamFactory = streamFactory;
	}

	public InputStreamFactory getStreamFactory() throws SQLException {
		if (this.streamFactory == null) {
    		throw new SQLException("Already freed"); //$NON-NLS-1$
    	}
		return streamFactory;
	}
	
	public void free() throws SQLException {
		if (this.streamFactory != null) {
			try {
				this.streamFactory.free();
				this.streamFactory = null;
			} catch (IOException e) {
				SQLException ex = new SQLException(e.getMessage());
				ex.initCause(e);
				throw ex;
			}
		}
	}
	
    public Reader getCharacterStream() throws SQLException {
    	try {
			return this.getStreamFactory().getCharacterStream();
		} catch (IOException e) {
			SQLException ex = new SQLException(e.getMessage());
			ex.initCause(e);
			throw ex;
		}
    }

    public InputStream getBinaryStream() throws SQLException {
    	try {
			return this.getStreamFactory().getInputStream();
		} catch (IOException e) {
			SQLException ex = new SQLException(e.getMessage());
			ex.initCause(e);
			throw ex;
		}
    }
    
    @Override
    public void readExternal(ObjectInput in) throws IOException,
    		ClassNotFoundException {
    	streamFactory = (InputStreamFactory)in.readObject();
    }
    
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
    	out.writeObject(streamFactory);
    }

}
