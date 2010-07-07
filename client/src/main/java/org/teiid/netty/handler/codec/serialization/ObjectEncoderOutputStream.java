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

import org.teiid.core.util.AccessibleByteArrayOutputStream;
import org.teiid.core.util.ExternalizeUtil;


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
    private final int estimatedLength;
    
    public ObjectEncoderOutputStream(DataOutputStream out, int estimatedLength) throws SecurityException, IOException {
    	super();
    	this.out = out;
        this.estimatedLength = estimatedLength;
    }
    
    @Override
    final protected void writeObjectOverride(Object obj) throws IOException {
        AccessibleByteArrayOutputStream baos = new AccessibleByteArrayOutputStream(estimatedLength);
        CompactObjectOutputStream oout = new CompactObjectOutputStream(baos);
        oout.writeObject(obj);
        ExternalizeUtil.writeCollection(oout, oout.getReferences());
        oout.flush();
        oout.close();
        
        out.writeInt(baos.getCount()); //includes the lob references
        out.write(baos.getBuffer(), 0, baos.getCount());
        byte[] chunk = new byte[(1 << 16) - 1];
        for (InputStream is : oout.getStreams()) {
        	while (true) {
	        	int bytes = is.read(chunk);
	        	out.writeShort(Math.max(0, bytes));
	        	if (bytes < 1) {
	        		is.close();
	        		break;
	        	}
	    		out.write(chunk, 0, bytes);
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
    }
    
}
