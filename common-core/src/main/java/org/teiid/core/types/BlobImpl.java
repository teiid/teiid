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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

import javax.sql.rowset.serial.SerialBlob;

import org.teiid.core.CorePlugin;
import org.teiid.core.types.LobSearchUtil.StreamProvider;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.SqlUtil;


/**
 * This object holds a chunk of binary data and implements the JDBC Blob interface.
 * It presents a streaming interface and provides a way to access partial
 * of the blob data. Connectors can use this object when dealing with large
 * objects.
 */
public class BlobImpl extends BaseLob implements Blob, StreamProvider {

    public BlobImpl() {

    }

    /**
     * Creates a MMBlob object with the <code>valueID</code>.
     * @param streamFactory reference to value chunk in data source.
     */
    public BlobImpl(InputStreamFactory streamFactory) {
        super(streamFactory);
    }

    /**
     * Returns as an array of bytes, part or all of the <code>BLOB</code>
     * value that this <code>Blob</code> object designates.  The byte
     * array contains up to <code>length</code> consecutive bytes
     * starting at position <code>pos</code>.
     * @param pos the ordinal position of the first byte in the
     * <code>BLOB</code> value to be extracted; the first byte is at
     * position 1
     * @param length the number of consecutive bytes to be copied
     * @return a byte array containing up to <code>length</code>
     * consecutive bytes from the <code>BLOB</code> value designated
     * by this <code>Blob</code> object, starting with the
     * byte at position <code>pos</code>
     * @exception SQLException if there is an error accessing the
     * <code>BLOB</code>
     */
    public byte[] getBytes(long pos, int length) throws SQLException {
        if (pos < 1) {
            Object[] params = new Object[] {pos};
            throw new SQLException(CorePlugin.Util.getString("MMClob_MMBlob.2", params)); //$NON-NLS-1$
        }
        else if (length == 0 || pos > length()) {
            return new byte[0];
        }
        pos = pos - 1;

        if (length < 0) {
            Object[] params = new Object[] {length};
            throw new SQLException(CorePlugin.Util.getString("MMClob_MMBlob.3", params)); //$NON-NLS-1$
        }
        else if (pos + length > length()) {
            length = (int)(length() - pos);
        }
        InputStream in = getBinaryStream();
        try {
            long skipped = 0;
            while (pos > 0) {
                skipped = in.skip(pos);
                pos -= skipped;
            }
            return ObjectConverterUtil.convertToByteArray(in, length);
        } catch (IOException e) {
            throw new SQLException(e);
        } finally {
            try {
                in.close();
            } catch (IOException e) {
            }
        }
    }

    /**
     * Determines the byte position in the <code>BLOB</code> value
     * designated by this <code>Blob</code> object at which
     * <code>pattern</code> begins.  The search begins at position
     * <code>start</code>.
     * @param pattern the <code>Blob</code> object designating
     * the <code>BLOB</code> value for which to search
     * @param start the position in the <code>BLOB</code> value
     *        at which to begin searching; the first position is 1
     * @return the position at which the pattern begins, else -1
     * @exception SQLException if there is an error accessing the
     * <code>BLOB</code>
     */
    public long position(final Blob pattern, long start) throws SQLException {
        if (pattern == null) {
            return -1;
        }

        return LobSearchUtil.position(new LobSearchUtil.StreamProvider() {
            public InputStream getBinaryStream() throws SQLException {
                return pattern.getBinaryStream();
            }
        }, pattern.length(), this, this.length(), start, 1);
    }

    /**
     * Determines the byte position at which the specified byte
     * <code>pattern</code> begins within the <code>BLOB</code>
     * value that this <code>Blob</code> object represents.  The
     * search for <code>pattern</code> begins at position
     * <code>start</code>.
     * @param pattern the byte array for which to search
     * @param start the position at which to begin searching; the
     *        first position is 1
     * @return the position at which the pattern appears, else -1
     * @exception SQLException if there is an error accessing the
     * <code>BLOB</code>
     */
    public long position(byte[] pattern, long start) throws SQLException {
        if (pattern == null) {
            return -1;
        }
        return position(new SerialBlob(pattern), start);
    }

    public InputStream getBinaryStream(long arg0, long arg1)
            throws SQLException {
        throw SqlUtil.createFeatureNotSupportedException();
    }


    /**
     * @see java.sql.Blob#setBytes(long, byte[])
     */
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        throw SqlUtil.createFeatureNotSupportedException();
    }

    /**
     * @see java.sql.Blob#setBytes(long, byte[], int, int)
     */
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        throw SqlUtil.createFeatureNotSupportedException();
    }

    /**
     * @see java.sql.Blob#setBinaryStream(long)
     */
    public OutputStream setBinaryStream(long pos) throws SQLException {
        throw SqlUtil.createFeatureNotSupportedException();
    }

    /**
     * @see java.sql.Blob#truncate(long)
     */
    public void truncate(long len) throws SQLException {
        throw SqlUtil.createFeatureNotSupportedException();
    }

    /**
     * For a given blob try to determine the length without fully reading an inputstream
     * @return the length or -1 if it cannot be determined
     */
    public static long quickLength(Blob b) {
        if (b instanceof BlobType) {
            BlobType blob = (BlobType)b;
            long length = blob.getLength();
            if (length != -1) {
                return length;
            }
            return quickLength(blob.getReference());
        }
        if (b instanceof BlobImpl) {
            BlobImpl blob = (BlobImpl)b;
            try {
                return blob.getStreamFactory().getLength();
            } catch (SQLException e) {
                return -1;
            }
        }
        try {
            return b.length();
        } catch (SQLException e) {
            return -1;
        }
    }
}
