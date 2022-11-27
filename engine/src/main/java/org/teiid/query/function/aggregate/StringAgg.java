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

package org.teiid.query.function.aggregate;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import javax.sql.rowset.serial.SerialBlob;

import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.FileStore.FileStoreOutputStream;
import org.teiid.common.buffer.FileStoreInputStreamFactory;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.Streamable;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.query.QueryPlugin;
import org.teiid.query.util.CommandContext;

/**
 * Aggregates binary and character strings
 */
public class StringAgg extends AggregateFunction {

    private FileStoreInputStreamFactory result;
    private boolean binary;

    public StringAgg(boolean binary) {
        this.binary = binary;
    }

    private FileStoreInputStreamFactory buildResult(CommandContext context) {
        FileStore fs = context.getBufferManager().createFileStore("string_agg"); //$NON-NLS-1$
        FileStoreInputStreamFactory fisf = new FileStoreInputStreamFactory(fs, Streamable.ENCODING);
        return fisf;
    }

    public void reset() {
        this.result = null;
    }

    @Override
    public void addInputDirect(List<?> tuple, CommandContext commandContext)
            throws TeiidComponentException, TeiidProcessingException {
        boolean first = false;
        if (result == null) {
            first = true;
            result = buildResult(commandContext);
        }
        if (!first) {
            Object delim = tuple.get(argIndexes[1]);
            writeValue(delim);
        }
        Object val = tuple.get(argIndexes[0]);
        writeValue(val);
    }

    private void writeValue(Object val) throws TeiidProcessingException {
        try {
            if (binary) {
                if (val instanceof BinaryType) {
                    result.getOuputStream().write(((BinaryType)val).getBytesDirect());
                    return;
                }
                Blob b = (Blob)val;
                InputStream binaryStream = b.getBinaryStream();
                try {
                    ObjectConverterUtil.write(result.getOuputStream(), binaryStream, -1, false);
                } finally {
                    binaryStream.close();
                }
            } else {
                if (val instanceof String) {
                    result.getWriter().write((String)val);
                    return;
                }
                Clob c = (Clob)val;
                Reader characterStream = c.getCharacterStream();
                try {
                    ObjectConverterUtil.write(result.getWriter(), characterStream, -1, false);
                } finally {
                    characterStream.close();
                }
            }
        } catch (IOException e) {
            throw new TeiidProcessingException(QueryPlugin.Event.TEIID30422, e);
        } catch (SQLException e) {
            throw new TeiidProcessingException(QueryPlugin.Event.TEIID30423, e);
        }
    }

    /**
     * @see org.teiid.query.function.aggregate.AggregateFunction#getResult(CommandContext)
     */
    public Object getResult(CommandContext commandContext) throws TeiidProcessingException{
        if (this.result == null) {
            return null;
        }

        try {
            this.result.getWriter().close();
            FileStoreOutputStream fs = this.result.getOuputStream();
            fs.close();

            if (binary) {
                if (fs.bytesWritten()) {
                    return new BlobType(new BlobImpl(result));
                }
                return new BlobType(new SerialBlob(Arrays.copyOf(fs.getBuffer(), fs.getCount())));
            }
            if (fs.bytesWritten()) {
                return new ClobType(new ClobImpl(result, -1));
            }
            return new ClobType(new ClobImpl(new String(Arrays.copyOf(fs.getBuffer(), fs.getCount()), Streamable.ENCODING)));
        } catch (IOException e) {
            throw new TeiidProcessingException(QueryPlugin.Event.TEIID30422, e);
        }  catch (SQLException e) {
            throw new TeiidProcessingException(QueryPlugin.Event.TEIID30423, e);
        }
    }
}