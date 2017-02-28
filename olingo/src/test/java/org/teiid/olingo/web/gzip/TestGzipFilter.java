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

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Objects;

public class TestGzipFilter{

    @Test
    public void testUseGzipMessageRequest() throws IOException, ServletException{
        HttpServletRequest req = mockRequest("GZIP", "deflate");
        HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
        FilterChain chain = Mockito.mock(FilterChain.class);
        new GzipFilter().doFilter(req, res, chain);
        Mockito.verify(chain).doFilter(Mockito.any(GzipMessageRequest.class), Mockito.same(res));
        Mockito.verify(req, Mockito.times(2)).getHeader(Mockito.anyString());
        Mockito.verify(req).getCharacterEncoding();
        Mockito.verifyNoMoreInteractions(chain, req, res);
    }

    @Test
    public void testUseGzipMessageResponse() throws IOException, ServletException{
        HttpServletRequest req = mockRequest(null, "deflate,GZIP");
        HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
        FilterChain chain = Mockito.mock(FilterChain.class);
        new GzipFilter().doFilter(req, res, chain);
        Mockito.verify(chain).doFilter(Mockito.same(req), Mockito.any(GzipMessageResponse.class));
        Mockito.verify(req, Mockito.times(2)).getHeader(Mockito.anyString());
        Mockito.verify(res).getCharacterEncoding();
        Mockito.verify(res).setHeader(Mockito.argThat(new IgnoreCaseStringMatcher("Content-Encoding")),
                Mockito.argThat(new IgnoreCaseStringMatcher("gzip")));
        Mockito.verifyNoMoreInteractions(chain, req, res);
    }

    @Test
    public void testNoInteraction() throws IOException, ServletException{
        HttpServletRequest req = mockRequest(null, null);
        HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
        FilterChain chain = Mockito.mock(FilterChain.class);
        new GzipFilter().doFilter(req, res, chain);
        Mockito.verify(chain).doFilter(Mockito.same(req), Mockito.same(res));
    }

    private HttpServletRequest mockRequest(String contentEncoding, String acceptEncoding){
        HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
        Mockito.doReturn(contentEncoding).when(req).getHeader(Mockito.argThat(new IgnoreCaseStringMatcher("Content-Encoding")));
        Mockito.doReturn(acceptEncoding).when(req).getHeader(Mockito.argThat(new IgnoreCaseStringMatcher("Accept-Encoding")));
        return req;
    }

    private class IgnoreCaseStringMatcher extends BaseMatcher<String>{
        private final String toMatch;

        private IgnoreCaseStringMatcher(String toMatch){
            this.toMatch = toMatch;
        }

        @Override
        public boolean matches(Object item){
            return toMatch.equalsIgnoreCase(Objects.toString(item));
        }

        @Override
        public void describeTo(Description description){
            description.appendText(toMatch);
        }
    }
}
