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

package org.teiid.dqp.internal.process;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;

import org.teiid.client.lob.LobChunk;
import org.teiid.client.util.ResultsReceiver;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.BaseClobType;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.Streamable;
import org.teiid.core.types.XMLType;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;


public class LobWorkItem implements Runnable {

    private RequestWorkItem parent;
    private int chunkSize;

    /* private work item state */
    private String streamId;
    private ByteLobChunkStream stream;
    private int streamRequestId;
    private ResultsReceiver<LobChunk> resultsReceiver;

    public LobWorkItem(RequestWorkItem parent, DQPCore dqpCore, String streamId, int streamRequestId) {
        this.chunkSize = dqpCore.getChunkSize();
        this.streamId = streamId;
        this.parent = parent;
        this.streamRequestId = streamRequestId;
    }

    public void run() {
        LobChunk chunk = null;
        Exception ex = null;
        boolean shouldClose = false;

        try {
            // If no previous stream is not found for this request create one and
            // save for future
            if (stream == null) {
                stream = createLobStream(streamId);
            }

            // now get the chunk from stream
            chunk = stream.getNextChunk();
            parent.dataBytes.addAndGet(chunk.getBytes().length);
            shouldClose = chunk.isLast();
        } catch (TeiidComponentException e) {
            LogManager.logWarning(org.teiid.logging.LogConstants.CTX_DQP, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30027));
            ex = e;
        } catch (IOException|SQLException e) {
            //treat this as a processing exception
            ex = new TeiidProcessingException(e);
        }

        synchronized (this) {
            if (ex != null) {
                resultsReceiver.exceptionOccurred(ex);
                shouldClose = true;
            } else {
                resultsReceiver.receiveResults(chunk);
            }
            resultsReceiver = null;
        }

        if (shouldClose) {
            close();
        }
    }

    void close() {
        try {
            if (stream != null) {
                stream.close();
            }
        } catch (IOException e) {
            LogManager.logDetail(org.teiid.logging.LogConstants.CTX_DQP, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30027));
        }
        parent.removeLobStream(streamRequestId);
    }

    /**
     * Create a object which can create a sequence of LobChunk objects on a given
     * LOB object
     * @throws SQLException
     */
    private ByteLobChunkStream createLobStream(String referenceStreamId)
        throws TeiidComponentException, SQLException {

        // get the reference object in the buffer manager, and try to stream off
        // the original sources.
        Streamable<?> streamable = parent.resultsBuffer.getLobReference(referenceStreamId);

        if (streamable instanceof XMLType) {
            XMLType xml = (XMLType)streamable;
            return new ByteLobChunkStream(xml.getBinaryStream(), chunkSize);
        }
        else if (streamable instanceof BaseClobType) {
            BaseClobType clob = (BaseClobType)streamable;
            return new ByteLobChunkStream(new ReaderInputStream(clob.getCharacterStream(), Charset.forName(Streamable.ENCODING)), chunkSize);
        }
        BlobType blob = (BlobType)streamable;
        return new ByteLobChunkStream(blob.getBinaryStream(), chunkSize);
    }

    synchronized void setResultsReceiver(ResultsReceiver<LobChunk> resultsReceiver) {
        Assertion.isNull(this.resultsReceiver, "Cannot request results with a pending request"); //$NON-NLS-1$
        this.resultsReceiver = resultsReceiver;
    }

}
