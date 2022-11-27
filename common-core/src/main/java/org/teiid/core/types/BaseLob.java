/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.core.types;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OptionalDataException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.sql.SQLException;

import org.teiid.core.types.InputStreamFactory.StreamFactoryReference;
import org.teiid.core.util.InputStreamReader;

public class BaseLob implements Externalizable, StreamFactoryReference {

    private static final long serialVersionUID = -1586959324208959519L;
    private InputStreamFactory streamFactory;
    private Charset charset;

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

    public void setEncoding(String encoding) {
        if (encoding != null) {
            this.charset = Charset.forName(encoding);
        } else {
            this.charset = null;
        }
    }

    public Charset getCharset() {
        return charset;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    public void free() {
        //we don't actually free the underlying streamFactory as this could be a caching scenario
        this.streamFactory = null;
    }

    public Reader getCharacterStream() throws SQLException {
        try {
            Reader r = this.getStreamFactory().getCharacterStream();
            if (r != null) {
                return r;
            }
        } catch (IOException e) {
            SQLException ex = new SQLException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        Charset cs = getCharset();
        if (cs == null) {
            cs = Streamable.CHARSET;
        }
        return new InputStreamReader(getBinaryStream(), cs.newDecoder());
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
        try {
            charset = (Charset) in.readObject();
        } catch (EOFException e) {
            //just ignore
        } catch (OptionalDataException e) {
            //just ignore
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(streamFactory);
        out.writeObject(charset);
    }

    /**
     * Returns the number of bytes.
     */
    public long length() throws SQLException{
        if (getStreamFactory().getLength() == -1) {
            getStreamFactory().setLength(length(getBinaryStream()));
        }
        return getStreamFactory().getLength();
    }

    static long length(InputStream is) throws SQLException {
        if (!(is instanceof BufferedInputStream)) {
            is = new BufferedInputStream(is);
        }
        try {
            long length = 0;
            while (is.read() != -1) {
                length++;
            }
            return length;
        } catch (IOException e) {
            throw new SQLException(e);
        } finally {
            try {
                is.close();
            } catch (IOException e) {
            }
        }
    }

}
