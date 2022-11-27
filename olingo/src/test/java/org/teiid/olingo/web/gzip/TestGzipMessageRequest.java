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
package org.teiid.olingo.web.gzip;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestGzipMessageRequest{

    private GzipMessageRequest request;
    private ServletInputStream stream;
    private ByteBuffer streamBytes;

    @Before
    public void prepareRequest() throws IOException{
        streamBytes = TestGzipMessageResponse.TEST_STRING_IN_GZIP.duplicate();
        stream = new ServletInputStream(){
            @Override public boolean isReady(){ return true; }
            @Override public boolean isFinished(){ return false; }
            @Override public void setReadListener(ReadListener readListener){ }
            @Override public int read() throws IOException{ return streamBytes.remaining()==0?-1:(streamBytes.get()&0xff); }
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
        Assert.assertEquals("There should be no more bytes in stream.", 0, streamBytes.remaining());
    }

    @Test
    public void testReadFromInputStream() throws Exception{
        ServletInputStream sis = request.getInputStream();
        byte[] buff = new byte[TestGzipMessageResponse.TEST_STRING.getBytes().length];
        sis.read(buff);
        Assert.assertEquals("Expected String output.", TestGzipMessageResponse.TEST_STRING, new String(buff));
        Assert.assertEquals("There should be no more bytes in stream.", 0, streamBytes.remaining());
    }

    private static HttpServletRequest mockRequest(ServletInputStream stream) throws IOException{
        HttpServletRequest out = Mockito.mock(HttpServletRequest.class);
        Mockito.doReturn(stream).when(out).getInputStream();
        Mockito.doThrow(Error.class).when(out).getReader();
        return out;
    }
}
