/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.common.net;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import junit.framework.TestCase;

import com.metamatrix.common.comm.platform.socket.SocketUtil;
import com.metamatrix.core.util.UnitTestUtil;

public class TestSocketHelper extends TestCase {
    
    public void testInternalHandshake() throws Exception {
    	InetAddress addr = new InetSocketAddress(0).getAddress();
    	ServerSocketConfiguration helper = new ServerSocketConfiguration();
    	final ServerSocket serverSocket = helper.getInternalServerSocket(0, 50, addr);
    	String hello = "hello"; //$NON-NLS-1$
    	final byte[] result = new byte[hello.getBytes().length]; 
    	Thread t = new Thread() {
    		@Override
    		public void run() {
    			try {
	    			Socket s = serverSocket.accept();
	    			InputStream is = s.getInputStream();
	    			synchronized (result) {
		    			is.read(result);
		    			result.notify();
					}
    			} catch (Exception e) {
    				e.printStackTrace();
    			}
    		}
    	};
    	t.start();
    	
    	Socket clientSocket = helper.getInternalClientSocket(addr, serverSocket.getLocalPort());
    	OutputStream os = clientSocket.getOutputStream();
    	os.write(hello.getBytes());
    	os.flush();
    	synchronized (result) {
    		if (result[0] == 0) {
    			result.wait(500);
    		}
		}
    	assertEquals(hello, new String(result));
    }
}
