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
package org.teiid.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.net.socket.ServiceInvocationStruct;
import org.teiid.odbc.ODBCServerRemote;
import org.teiid.runtime.RuntimePlugin;

/**
 * Represents the messages going from PG ODBC Client --> back end Server  
 * Some parts of this code is taken from H2's implementation of ODBC
 */
@SuppressWarnings("nls")
public class PgFrontendProtocol extends ByteToMessageDecoder {

	private static final int LO_CREAT =	957;
	private static final int LO_OPEN = 952;
	private static final int LO_CLOSE = 953;
	private static final int LO_READ = 954;
	private static final int LO_WRITE = 955;
	private static final int LO_LSEEK = 956;
	private static final int LO_TELL = 958;
	private static final int LO_UNLINK = 964;
	
	private int maxObjectSize;
	private Byte messageType;
	private Integer dataLength;
	private boolean initialized = false;
	private ODBCServerRemote odbcProxy;
	private PGRequest message;
	private String user;
	private String databaseName;
	private PgBackendProtocol pgBackendProtocol;
	
	public PgFrontendProtocol(PgBackendProtocol pgBackendProtocol, int maxObjectSize) {
        
		if (maxObjectSize <= 0) {
            throw new IllegalArgumentException("maxObjectSize: " + maxObjectSize); //$NON-NLS-1$
        }
		
		this.maxObjectSize = maxObjectSize;
		this.pgBackendProtocol = pgBackendProtocol;
		// the proxy is used for generating the object based message based on ServiceInvocationStruct class.
		this.odbcProxy = (ODBCServerRemote)Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] {ODBCServerRemote.class}, new InvocationHandler() {
			@Override
			public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
				if (LogManager.isMessageToBeRecorded(LogConstants.CTX_ODBC, MessageLevel.TRACE)) {
					LogManager.logTrace(LogConstants.CTX_ODBC, "invoking server method:", method.getName(), Arrays.deepToString(args)); //$NON-NLS-1$
				}
				message = new PGRequest();
				message.struct = new ServiceInvocationStruct(args, method.getName(),ODBCServerRemote.class);
				return null;
			}
		});
	}
	
	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) throws Exception {
		
        if (this.initialized && this.messageType == null) {
	        if (buffer.readableBytes() < 1 ) {
	            return ;
	        }
	
	        this.messageType = buffer.readByte();
	        if (this.messageType < 0 ) {
	        	this.odbcProxy.terminate();
	        	out.add(this.message);
	        }
        }
        
        if (!this.initialized) {
        	this.messageType = 'I';
        }
        
        if (this.dataLength == null) {
	        if (buffer.readableBytes() < 4) {
	            return ;
	        }
	                
	        this.dataLength = buffer.readInt();
	        if (this.dataLength <= 0) {
	            throw new StreamCorruptedException("invalid data length: " + this.dataLength); //$NON-NLS-1$
	        }
	        if (this.dataLength > this.maxObjectSize) {
	            throw new StreamCorruptedException("data length too big: " + this.dataLength + " (max: " + this.maxObjectSize + ')'); //$NON-NLS-1$ //$NON-NLS-2$
	        }
        }

        if (buffer.readableBytes() < this.dataLength - 4) {
            return;
        }

        byte[] data = createByteArray(this.dataLength - 4);
        buffer.readBytes(data);
        createRequestMessage(
                this.messageType,
                new NullTerminatedStringDataInputStream(data,
                        new DataInputStream(new ByteArrayInputStream(data, 0,this.dataLength - 4)), 
                        this.pgBackendProtocol.getEncoding()), ctx.channel());
		this.dataLength = null;
		this.messageType = null;
		out.add(this.message);
	}

	private Object createRequestMessage(byte messageType, NullTerminatedStringDataInputStream data, Channel channel) throws IOException{
        switch(messageType) {
        case 'I': 
        	this.initialized = true;
        	return buildInitialize(data, channel);
        case 'p':
        	return buildLogin(data, channel);
        case 'P':
        	return buildParse(data);
        case 'B':
        	return buildBind(data);
        case 'E':
        	return buildExecute(data);
        case 'Q':
        	return buildExecuteQuery(data);
        case 'D':
        	return buildDescribe(data);
        case 'X':
        	return buildTeminate();
        case 'S':
        	return buildSync();
        case 'C':
        	return buildClose(data);
        case 'H':
        	return buildFlush();
        case 'F':
        	return buildFunctionCall(data);        	               	
        default:
        	return buildError();
        }
	}

	private Object buildError() {
		this.odbcProxy.unsupportedOperation("option not suported");
		return message;
	}

	private Object buildFlush() {
		this.odbcProxy.flush();
		return message;
	}

	private Object buildTeminate() {
		this.odbcProxy.terminate();
		return message;
	}

	private Object buildInitialize(NullTerminatedStringDataInputStream data, Channel channel) throws IOException{
        Properties props = new Properties();
       
        int version = data.readInt();
        props.setProperty("version", Integer.toString(version));
        
        // SSL Request
        if (version == 80877103) {
        	this.initialized = false;
        	this.odbcProxy.sslRequest();
        	return message;
        }
        
        if (this.pgBackendProtocol.secureData() && channel.pipeline().get(org.teiid.transport.PgBackendProtocol.SSL_HANDLER_KEY) == null) {
        	this.odbcProxy.unsupportedOperation(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40123));
        	return message;
        }
        
        trace("StartupMessage version", version, "(", (version >> 16), ".", (version & 0xff), ")");
        
        while (true) {
            String param =  data.readString();
            if (param.length() == 0) {
                break;
            }
            String value =  data.readString();
            props.setProperty(param, value);
        }        
        this.user = props.getProperty("user");
        this.databaseName = props.getProperty("database");
        String clientEncoding = props.getProperty("client_encoding", PgBackendProtocol.DEFAULT_ENCODING);
        props.setProperty("client_encoding", clientEncoding);
        props.setProperty("default_transaction_isolation", "read committed");
        props.setProperty("integer_datetimes", "on");
        props.setProperty("DateStyle", "ISO");
        props.setProperty("TimeZone", Calendar.getInstance().getTimeZone().getDisplayName());
        this.odbcProxy.initialize(props);
        return message;
	}
	
	private Object buildLogin(NullTerminatedStringDataInputStream data, Channel channel) {
        this.odbcProxy.logon(this.databaseName, this.user, data, channel.remoteAddress());
        return message;
	}	

	private Object buildParse(NullTerminatedStringDataInputStream data) throws IOException {
        String name = data.readString();
        String sql = data.readString();
        
        //The number of parameter data types specified (can be zero). Note that this is not 
        //an indication of the number of parameters that might appear in the query string, only 
        //the number that the frontend wants to prespecify types for.
        int count = data.readShort();
        int[] paramType = new int[count];
        for (int i = 0; i < count; i++) {
            int type = data.readInt();
            paramType[i] = type;
        }
        this.odbcProxy.prepare(name, sql, paramType);
        return message;
	}	

	private Object buildBind(NullTerminatedStringDataInputStream data) throws IOException {
        String bindName = data.readString();
        String prepName = data.readString();
        
        int formatCodeCount = data.readShort();
        int[] formatCodes = new int[formatCodeCount];
        for (int i = 0; i < formatCodeCount; i++) {
            formatCodes[i] = data.readShort();
        }
        
        int paramCount = data.readShort();
        Object[] params = new Object[paramCount];
        for (int i = 0; i < paramCount; i++) {
            int paramLen = data.readInt();
            byte[] paramdata = createByteArray(paramLen);
            data.readFully(paramdata);
            
            // the params can be either text or binary
            if (formatCodeCount == 0 || (formatCodeCount == 1 && formatCodes[0] == 0) || formatCodes[i] == 0) {
            	params[i] = new String(paramdata, this.pgBackendProtocol.getEncoding());
            }
            else {
            	params[i] = paramdata;
            }
        }
        
        int resultCodeCount = data.readShort();
        short[] resultColumnFormat = null;
        if (resultCodeCount != 0) {
            resultColumnFormat = new short[resultCodeCount];
            for (int i = 0; i < resultCodeCount; i++) {
                resultColumnFormat[i] = data.readShort();
            }
        }
        this.odbcProxy.bindParameters(bindName, prepName, params, resultCodeCount, resultColumnFormat, this.pgBackendProtocol.getEncoding());
        return message;
	}	

	private Object buildExecute(NullTerminatedStringDataInputStream data) throws IOException {
		String portalName = data.readString();
        int maxRows = data.readInt();
        this.odbcProxy.execute(portalName, maxRows);
        return message;
	}	


	private Object buildDescribe(NullTerminatedStringDataInputStream data)  throws IOException{
        char type = (char) data.readByte();
        String name = data.readString();
        if (type == 'S') {
        	this.odbcProxy.getParameterDescription(name);
        	return message;
        } else if (type == 'P') {
        	this.odbcProxy.getResultSetMetaDataDescription(name);
        	return message;
        } else {
            trace("expected S or P, got ", type);
            this.odbcProxy.unsupportedOperation("expected S or P");
            return message;
        }
	}
	

	private Object buildSync() {
		this.odbcProxy.sync();
		return message;
	}	

	private Object buildExecuteQuery(NullTerminatedStringDataInputStream data) throws IOException {
        String query = data.readString();
        this.odbcProxy.executeQuery(query);
        return message;
	}	
	
	static byte[] createByteArray(int length) throws StreamCorruptedException{
		try {
			return new byte[length];
		} catch(OutOfMemoryError e) {
			throw new StreamCorruptedException("data too big: " + e.getMessage()); //$NON-NLS-1$ 
		}
	}
	
	private Object buildClose(NullTerminatedStringDataInputStream data) throws IOException {
		char type = (char)data.read();
		String name = data.readString();
		if (type == 'S') {
			this.odbcProxy.closePreparedStatement(name);
		}
		else if (type == 'P') {
			this.odbcProxy.closeBoundStatement(name);
		}
		else {
			this.odbcProxy.unsupportedOperation("unknown close type specified");
		}
		return message;
	}	
	
	/**
	 * LO functions are always binary, so I am ignoring the formats, return types. The below is not used
	 * leaving for future if ever LO is revisited
	 */
	@SuppressWarnings("unused")
	private Object buildFunctionCall(NullTerminatedStringDataInputStream data) throws IOException {
		int funcID = data.readInt();
		
		// read data types of arguments 
		int formatCount = data.readShort();
		int[] formatTypes = new int[formatCount];
		for (int i = 0; i< formatCount; i++) {
			formatTypes[i] = data.readShort();
		}
		
		// arguments	
		data.readShort(); // ignore the param count; we know them by functions supported.
		int oid = readInt(data);
		switch(funcID) {
		case LO_CREAT:
			break;
		case LO_OPEN:
			int mode = readInt(data);
			break;
		case LO_CLOSE:
			break;			
		case LO_READ:
			int length = readInt(data);
			break;
		case LO_WRITE:
			byte[] contents = readByteArray(data);
			break;
		case LO_LSEEK:
			int offset = readInt(data);
			int where = readInt(data);
			break;
		case LO_TELL:
			break;
		case LO_UNLINK:			
			break;		
		}
		this.odbcProxy.functionCall(oid);
		return message;
	}	
	
	private int readInt(NullTerminatedStringDataInputStream data) throws IOException {
		data.readInt(); // ignore this this is length always 4
		return data.readInt();
	}
	
	private byte[] readByteArray(NullTerminatedStringDataInputStream data) throws IOException {
		int length = data.readInt();
		if (length == -1 || length == 0) {
			return null;
		}
		byte[] content = createByteArray(length);
		data.read(content, 0, length);
		return content;
	}
	
	public static class PGRequest {
		ServiceInvocationStruct struct;
	}
	
	public static class NullTerminatedStringDataInputStream extends DataInputStream{
		private Charset encoding;
		private byte[] rawData;
		
		public NullTerminatedStringDataInputStream(byte[] rawData, DataInputStream in, Charset encoding) {
			super(in);
			this.encoding = encoding;
			this.rawData = rawData;
		}

	    public String readString() throws IOException {
	        ByteArrayOutputStream buff = new ByteArrayOutputStream();
	        while (true) {
	            int x = read();
	            if (x <= 0) {
	                break;
	            }
	            buff.write(x);
	        }
	        return new String(buff.toByteArray(), this.encoding);
	    }
	    
	    public byte[] readServiceToken() {
	    	return this.rawData;
	    }
	}
	
	private static void trace(Object... msg) {
		LogManager.logTrace(LogConstants.CTX_ODBC, msg);
	}
}
