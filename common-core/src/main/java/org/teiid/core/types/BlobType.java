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

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

import javax.sql.rowset.serial.SerialBlob;

import org.teiid.core.CorePlugin;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.ObjectConverterUtil;


/**
 * Represent a value of type "blob", which can be streamable from client
 */
public class BlobType extends Streamable<Blob> implements Blob, Comparable<BlobType> {

    private static final long serialVersionUID = 1294191629070433450L;

    public BlobType() {
    }

    public BlobType(byte[] bytes) {
        super(createBlob(bytes));
    }

    public BlobType(Blob blob) {
        super(blob);
    }

    /**
     * @see java.sql.Blob#getBinaryStream()
     */
    public InputStream getBinaryStream() throws SQLException {
        return this.reference.getBinaryStream();
    }

    /**
     * @see java.sql.Blob#getBytes(long, int)
     */
    public byte[] getBytes(long pos, int len) throws SQLException {
        return this.reference.getBytes(pos, len);
    }

    @Override
    long computeLength() throws SQLException {
        return this.reference.length();
    }

    /**
     * @see java.sql.Blob#position(java.sql.Blob, long)
     */
    public long position(Blob pattern, long start) throws SQLException {
        return this.reference.position(pattern, start);
    }

    /**
     * @see java.sql.Blob#position(byte[], long)
     */
    public long position(byte[] pattern, long start) throws SQLException {
        return this.reference.position(pattern, start);
    }

    /**
     * @see java.sql.Blob#setBinaryStream(long)
     */
    public OutputStream setBinaryStream(long pos) throws SQLException {
        return this.reference.setBinaryStream(pos);
    }

    /**
     * @see java.sql.Blob#setBytes(long, byte[], int, int)
     * @since 4.3
     */
    public int setBytes(long pos,
                        byte[] bytes,
                        int offset,
                        int len) throws SQLException {
        return this.reference.setBytes(pos, bytes, offset, len);
    }

    /**
     * @see java.sql.Blob#setBytes(long, byte[])
     */
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        return this.reference.setBytes(pos, bytes);
    }

    /**
     * @see java.sql.Blob#truncate(long)
     */
    public void truncate(long len) throws SQLException {
        this.reference.truncate(len);
    }

    public void free() throws SQLException {
        this.reference.free();
    }

    public InputStream getBinaryStream(long pos, long len)
            throws SQLException {
        return this.reference.getBinaryStream(pos, len);
    }

    public static SerialBlob createBlob(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            return new SerialBlob(bytes);
        } catch (SQLException e) {
              throw new TeiidRuntimeException(CorePlugin.Event.TEIID10047, e);
        }
    }

    @Override
    protected void readReference(ObjectInput in) throws IOException {
        byte[] bytes = new byte[(int)getLength()];
        in.readFully(bytes);
        try {
            this.reference = new SerialBlob(bytes);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void writeReference(final DataOutput out) throws IOException {
        try {
            writeBinary(out, getBinaryStream(), (int)length);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    static void writeBinary(final DataOutput out, InputStream is, int length) throws IOException {
        OutputStream os = new OutputStream() {

            @Override
            public void write(int b) throws IOException {
                out.write(b);
            }
        };
        try {
            int bytes = ObjectConverterUtil.write(os, is, length, false);
            if (bytes != length) {
                throw new IOException("Expected length " + length + " but was " + bytes); //$NON-NLS-1$ //$NON-NLS-2$
            }
        } finally {
            is.close();
        }
    }

    @Override
    public int compareTo(BlobType o) {
        try {
            InputStream is1 = this.getBinaryStream();
            InputStream is2 = o.getBinaryStream();
            long len1 = this.length();
            long len2 = o.length();
            long n = Math.min(len1, len2);
            for (long i = 0; i < n; i++) {
                int b1 = is1.read();
                int b2 = is2.read();
                if (b1 != b2) {
                    return b1 - b2;
                }
            }
            return Long.signum(len1 - len2);
        } catch (SQLException e) {
              throw new TeiidRuntimeException(CorePlugin.Event.TEIID10048, e);
        } catch (IOException e) {
              throw new TeiidRuntimeException(CorePlugin.Event.TEIID10049, e);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof BlobType)) {
            return false;
        }
        BlobType other = (BlobType)obj;
        if (EquivalenceUtil.areEqual(reference, other.reference)) {
            return true;
        }
        try {
            return this.compareTo(other) == 0;
        } catch (TeiidRuntimeException e) {
            return false;
        }
    }

    @Override
    public int hashCode() {
        try {
            return (int) reference.length();
        } catch (TeiidRuntimeException e) {
            return 0;
        } catch (SQLException e) {
            return 0;
        }
    }

}
