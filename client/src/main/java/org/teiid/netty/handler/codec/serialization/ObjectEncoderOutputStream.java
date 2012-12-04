/*
 * JBoss, Home of Professional Open Source
 *
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @author tags. See the COPYRIGHT.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.teiid.netty.handler.codec.serialization;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import org.teiid.core.util.ExternalizeUtil;
import org.teiid.core.util.MultiArrayOutputStream;


/**
 * An {@link ObjectOutput} which is interoperable with {@link ObjectDecoderInputStream}.
 *
 * @author The Netty Project (netty-dev@lists.jboss.org)
 * @author Trustin Lee (tlee@redhat.com)
 *
 * @version $Rev: 595 $, $Date: 2008-12-08 03:02:33 -0600 (Mon, 08 Dec 2008) $
 *
 */
public class ObjectEncoderOutputStream extends ObjectOutputStream {

    private final DataOutputStream out;
	private MultiArrayOutputStream baos;
    
    public ObjectEncoderOutputStream(DataOutputStream out, int initialBufferSize) throws SecurityException, IOException {
    	super();
    	this.out = out;
        baos = new MultiArrayOutputStream(initialBufferSize);
    }
    
    @Override
    final protected void writeObjectOverride(Object obj) throws IOException {
        baos.reset(4);
        CompactObjectOutputStream oout = new CompactObjectOutputStream(baos);
        oout.writeObject(obj);
        ExternalizeUtil.writeCollection(oout, oout.getReferences());
        oout.flush();
        oout.close();
        
        int val = baos.getCount()-4;
        byte[] b = baos.getBuffers()[0];
        b[3] = (byte) (val >>> 0);
    	b[2] = (byte) (val >>> 8);
    	b[1] = (byte) (val >>> 16);
    	b[0] = (byte) (val >>> 24);
    	baos.writeTo(out);
        
    	if (!oout.getStreams().isEmpty()) {
    		baos.reset(0);
    		byte[] chunk = new byte[(1 << 16)];
	        for (InputStream is : oout.getStreams()) {
	        	while (true) {
		        	int bytes = is.read(chunk, 2, chunk.length - 2);
		        	int toWrite = Math.max(0, bytes);
		        	chunk[1] = (byte) (toWrite >>> 0);
		        	chunk[0] = (byte) (toWrite >>> 8);
		        	if (baos.getIndex() + toWrite + 2 > b.length) {
		        		//exceeds the first buffer
			        	baos.writeTo(out);
			        	baos.reset(0);
			        	out.write(chunk, 0, toWrite + 2);
		        	} else {
		        		//buffer the small chunk
		        		baos.write(chunk, 0, toWrite + 2);
		        	}
		        	if (bytes < 1) {
		        		is.close();
		        		break;
		        	}
	        	}
			}
	        if (baos.getIndex() > 0) {
	        	baos.writeTo(out);
        	}
    	}
    }
    
    @Override
    public void close() throws IOException {
    	out.close();
    }
    
    @Override
    public void flush() throws IOException {
    	out.flush();
    }
    
    @Override
    public void reset() throws IOException {
    	//automatically resets with each use
    }
    
}
