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
import java.io.Writer;
import java.sql.Array;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import javax.sql.rowset.serial.SerialBlob;

import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.FileStore.FileStoreOutputStream;
import org.teiid.common.buffer.FileStoreInputStreamFactory;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.Streamable;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.symbol.DerivedColumn;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.TextLine;
import org.teiid.query.util.CommandContext;

/**
 * Aggregates Text entries
 */
public class TextAgg extends SingleArgumentAggregateFunction {

    private FileStoreInputStreamFactory result;
    private TextLine textLine;

    public TextAgg(TextLine textLine) {
        this.textLine = textLine;
    }

    private FileStoreInputStreamFactory buildResult(CommandContext context) throws TeiidProcessingException {
        try {
            FileStore fs = context.getBufferManager().createFileStore("textagg"); //$NON-NLS-1$
            FileStoreInputStreamFactory fisf = new FileStoreInputStreamFactory(fs, textLine.getEncoding()==null?Streamable.ENCODING:textLine.getEncoding());
            Writer w = fisf.getWriter();
            if (textLine.isIncludeHeader()) {
                Object[] header = TextLine.evaluate(textLine.getExpressions(), new TextLine.ValueExtractor<DerivedColumn>() {
                    public Object getValue(DerivedColumn t) {
                        if (t.getAlias() == null && t.getExpression() instanceof ElementSymbol) {
                            return ((ElementSymbol)t.getExpression()).getShortName();
                        }
                        return t.getAlias();
                    }
                }, textLine);
                writeList(w, header);
            }
            w.flush();
            return fisf;
        } catch (IOException e) {
            throw new TeiidProcessingException(QueryPlugin.Event.TEIID30420, e);
        }
    }

    private void writeList(Writer w, Object[] list) throws IOException {
        for (int i = 0; i < list.length; i++) {
            w.write(list[i].toString());
        }
    }

    public void reset() {
        this.result = null;
    }

    @Override
    public void addInputDirect(Object input, List<?> tuple, CommandContext commandContext) throws TeiidComponentException, TeiidProcessingException {
        try {
            if (this.result == null) {
                this.result = buildResult(commandContext);
            }
            Array in = (Array)input;
            Writer w = result.getWriter();
            writeList(w, (Object[])in.getArray());
            w.flush();
        } catch (SQLException e) {
            throw new TeiidProcessingException(QueryPlugin.Event.TEIID30421, e);
        } catch (IOException e) {
            throw new TeiidProcessingException(QueryPlugin.Event.TEIID30421, e);
        }
    }

    /**
     * @see org.teiid.query.function.aggregate.AggregateFunction#getResult(CommandContext)
     */
    public Object getResult(CommandContext commandContext) throws TeiidProcessingException{
        if (this.result == null) {
            this.result = buildResult(commandContext);
        }

        try {
            FileStoreOutputStream fs = this.result.getOuputStream();
            fs.close();

            if (fs.bytesWritten()) {
                return new BlobType(new BlobImpl(result));
            }
            return new BlobType(new SerialBlob(Arrays.copyOf(fs.getBuffer(), fs.getCount())));
        } catch (IOException e) {
            throw new TeiidProcessingException(QueryPlugin.Event.TEIID30422, e);
        }  catch (SQLException e) {
            throw new TeiidProcessingException(QueryPlugin.Event.TEIID30423, e);
        }
    }
}