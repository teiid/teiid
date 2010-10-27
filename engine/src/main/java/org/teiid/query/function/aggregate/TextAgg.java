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

package org.teiid.query.function.aggregate;

import java.io.IOException;
import java.io.Writer;
import java.sql.SQLException;
import java.util.List;

import javax.sql.rowset.serial.SerialClob;

import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.FileStore.FileStoreOutputStream;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.Streamable;
import org.teiid.query.processor.xml.XMLUtil.FileStoreInputStreamFactory;
import org.teiid.query.sql.symbol.TextLine;
import org.teiid.query.util.CommandContext;

/**
 * Aggregates Text entries
 */
public class TextAgg extends AggregateFunction {

    private FileStoreInputStreamFactory result;
    private CommandContext context;
    private TextLine textLine;
    
    public TextAgg(CommandContext context, TextLine textLine) {
    	this.context = context;
    	this.textLine = textLine;    	    	
	}

	private FileStoreInputStreamFactory buildResult(CommandContext context, TextLine textLine) throws TeiidProcessingException {
		try {
			FileStore fs = context.getBufferManager().createFileStore("textagg"); //$NON-NLS-1$
			FileStoreInputStreamFactory fisf = new FileStoreInputStreamFactory(fs, Streamable.ENCODING);
			Writer w = fisf.getWriter();
			if (textLine.isIncludeHeader()) {
				w.write(TextLine.getHeader(textLine.getExpressions(), textLine.getDelimiter(), textLine.getQuote()));
			}
			w.close();
			return fisf;
		} catch (IOException e) {
			throw new TeiidProcessingException(e);
		}
	}

    public void reset() {
    	this.result = null;
    }

    /**
     * @throws TeiidProcessingException 
     * @throws TeiidComponentException 
     * @see org.teiid.query.function.aggregate.AggregateFunction#addInputDirect(Object, List)
     */
    public void addInputDirect(Object input, List<?> tuple) throws TeiidComponentException, TeiidProcessingException {
    	try {
    		if (this.result == null) {
    			this.result = buildResult(this.context, this.textLine);
    		}
    		String in = (String)input;
    		Writer w = result.getWriter();
    		w.write(in);
			w.close();
		} catch (IOException e) {
			throw new TeiidProcessingException(e);
		}
    }

    /**
     * @see org.teiid.query.function.aggregate.AggregateFunction#getResult()
     */
    public Object getResult() throws TeiidProcessingException{
    	if (this.result == null) {
    		this.result = buildResult(this.context, this.textLine);
    	}
    	
    	try {
    		FileStoreOutputStream fs = this.result.getOuputStream();
			fs.close();
		
			if (fs.bytesWritten()) {
				return new ClobType(new ClobImpl(result, -1));
			}
			// fun convert bytes to string to char array!!
			String msg = new String(fs.getBuffer(),0, fs.getCount(), Streamable.ENCODING);
			return new ClobType(new SerialClob(msg.toCharArray()));
		} catch (IOException e) {
			throw new TeiidProcessingException(e);
		}  catch (SQLException e) {
			throw new TeiidProcessingException(e);
		}
    }
}