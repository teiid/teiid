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
