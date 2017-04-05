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
package org.teiid.olingo.web.gzip;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;

public class TestGzipMessageRequest{

    private GzipMessageRequest request;
    private ServletInputStream stream;
    private ByteArrayInputStream streamBytes;

    @Before
    public void prepareRequest() throws IOException{
        streamBytes = new ByteArrayInputStream(TestGzipMessageResponse.TEST_STRING_IN_GZIP);
        stream = new ServletInputStream(){
            @Override public boolean isReady(){ return true; }
            @Override public boolean isFinished(){ return false; }
            @Override public void setReadListener(ReadListener readListener){ }
            @Override public int read() throws IOException{ return streamBytes.read(); }
        };
        request = new GzipMessageRequest(mockRequest(stream));
    }

    @Test
    public void testIllegalGetInputStreamInvocation() throws IOException{
        request.getReader();
        try{
            request.getInputStream();
            Assert.fail("Expected " + IllegalStateException.class);
        } catch (IllegalStateException ex){/* expected */}
    }

    @Test
    public void testIllegalGetReaderInvocation() throws IOException{
        request.getInputStream();
        try{
            request.getReader();
            Assert.fail("Expected " + IllegalStateException.class);
        } catch (IllegalStateException ex){/* expected */}
    }

    @Test
    public void testReadFromReader() throws IOException{
        BufferedReader r = request.getReader();
        String read = r.readLine();
        Assert.assertEquals("Expected read String.", TestGzipMessageResponse.TEST_STRING, read);
        Assert.assertNull("No next line expected.", r.readLine());
        Assert.assertEquals("There should be no more bytes in stream.", 0, streamBytes.available());
    }

    @Test
    public void testReadFromInputStream() throws Exception{
        ServletInputStream sis = request.getInputStream();
        byte[] buff = new byte[TestGzipMessageResponse.TEST_STRING.getBytes().length];
        sis.read(buff);
        Assert.assertEquals("Expected String output.", TestGzipMessageResponse.TEST_STRING, new String(buff));
        Assert.assertEquals("There should be no more bytes in stream.", 0, streamBytes.available());
    }

    private static HttpServletRequest mockRequest(ServletInputStream stream) throws IOException{
        HttpServletRequest out = Mockito.mock(HttpServletRequest.class);
        Mockito.doReturn(stream).when(out).getInputStream();
        Mockito.doThrow(Error.class).when(out).getReader();
        return out;
    }
}
