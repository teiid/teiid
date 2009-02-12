/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.query.processor.xml;

import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.buffer.BufferManager.TupleSourceStatus;
import com.metamatrix.common.buffer.BufferManager.TupleSourceType;
import com.metamatrix.common.lob.BufferManagerLobChunkStream;
import com.metamatrix.common.lob.ByteLobChunkStream;
import com.metamatrix.common.lob.LobChunk;
import com.metamatrix.common.lob.LobChunkInputStream;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.SQLXMLImpl;
import com.metamatrix.common.types.XMLReaderFactory;
import com.metamatrix.common.types.XMLType;
import com.metamatrix.query.sql.symbol.ElementSymbol;


/** 
 * Utility methods to be used with the XML and XQuery processing.
 */
public class XMLUtil {

    /**
     * This method saves the given XML object to the buffer manager's disk process
     * and returns the id which is saved under.  
     */
    public static TupleSourceID saveToBufferManager(BufferManager bufferMgr, String tupleGroupName, SQLXML srcXML, int chunkSize) 
        throws MetaMatrixComponentException {        
        try{  
            // first persist the XML to the Buffer Manager
            TupleSourceID sourceId = createXMLTupleSource(bufferMgr, tupleGroupName);

            // since this object possibly will be streamed we need another streamable
            // wrapper layer.            
            ByteLobChunkStream lobStream = new ByteLobChunkStream(srcXML.getBinaryStream(), chunkSize);
            
            // read all the globs and Store into storage manager
            int batchCount = 1;
            LobChunk lobChunk = null;
            do {
                try {
                    lobChunk = lobStream.getNextChunk();
                } catch (IOException e) {
                	throw new MetaMatrixComponentException(e);
                }
                bufferMgr.addStreamablePart(sourceId, lobChunk, batchCount++);
            } while (!lobChunk.isLast());
            
            lobStream.close();
            
            bufferMgr.setStatus(sourceId, TupleSourceStatus.FULL);            
            return sourceId;            
        } catch (TupleSourceNotFoundException e) {
            throw new MetaMatrixComponentException(e);
        } catch(SQLException e) {
            throw new MetaMatrixComponentException(e);
        } catch(IOException e) {
            throw new MetaMatrixComponentException(e);
        } 
    }
    
    /**
     * This will reconstruct the XML object from the buffer manager from given 
     * buffer manager id. 
     */
    public static XMLType getFromBufferManager(final BufferManager bufferMgr, final TupleSourceID sourceId, Properties props) {
        SQLXML sqlXML = new SQLXMLImpl(new BufferMangerXMLReaderFactory(bufferMgr, sourceId), props);
        
        // this is object to be sent to the client. The reference
        // id will be set by the buffer manager.
        XMLType xml = new XMLType(sqlXML);
        xml.setPersistenceStreamId(sourceId.getStringID());
        
        return xml;
    }
    
    /**
     * Creates a buffer manager's id for XML based data. 
     */
    public static TupleSourceID createXMLTupleSource(BufferManager bufferMgr, String tupleGroupName) throws MetaMatrixComponentException {
        TupleSourceID sourceID = bufferMgr.createTupleSource(getOutputElements(), new String[]{DataTypeManager.DefaultDataTypes.XML}, tupleGroupName, TupleSourceType.PROCESSOR);
        return sourceID;
    }
    
    static List getOutputElements() {
        ArrayList output = new ArrayList(1);
        ElementSymbol xml = new ElementSymbol("xml"); //$NON-NLS-1$
        xml.setType(DataTypeManager.DefaultDataClasses.XML);
        output.add(xml);
        return output;
    }    
    
    /**
     * This is buffer manager supported reader factory for the XML lobs
     * so that XML can be streamed by the clients more than once. 
     */
    static class BufferMangerXMLReaderFactory implements XMLReaderFactory {
        BufferManager bufferMgr;
        TupleSourceID sourceId;
        
        BufferMangerXMLReaderFactory(BufferManager mgr, TupleSourceID id){
            this.bufferMgr = mgr;
            this.sourceId = id;
        }
        
        public Reader getReader() {          
            return new LobChunkInputStream(new BufferManagerLobChunkStream(this.sourceId.getStringID(), this.bufferMgr)).getUTF16Reader();
        }            
    }
}