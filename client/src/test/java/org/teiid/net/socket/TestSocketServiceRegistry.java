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

package org.teiid.net.socket;

import java.lang.reflect.Method;

import junit.framework.TestCase;

import org.teiid.client.DQP;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.client.util.ExceptionUtil;
import org.teiid.client.xa.XATransactionException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;

@SuppressWarnings("nls")
public class TestSocketServiceRegistry extends TestCase {

    interface Foo{
        void somemethod();
    }

    public void testExceptionConversionNoException() throws Exception {

        Method m = Foo.class.getMethod("somemethod", new Class[] {});

        Throwable t = ExceptionUtil.convertException(m, new TeiidComponentException());

        assertTrue(t instanceof TeiidRuntimeException);
    }

    public void testComponentExceptionConversion() throws Exception {

        Method m = DQP.class.getMethod("getMetadata", new Class[] {Long.TYPE});

        Throwable t = ExceptionUtil.convertException(m, new NullPointerException());

        assertTrue(t instanceof TeiidComponentException);
    }

    public void testXATransactionExceptionConversion() throws Exception {

        Method m = DQP.class.getMethod("recover", new Class[] {Integer.TYPE});

        Throwable t = ExceptionUtil.convertException(m, new TeiidComponentException());

        assertTrue(t instanceof XATransactionException);
    }

    public void testSubclass() throws Exception {

        Method m = DQP.class.getMethod("getMetadata", new Class[] {Long.TYPE});

        Throwable t = ExceptionUtil.convertException(m, new InvalidSessionException());

        assertTrue(t instanceof InvalidSessionException);
    }
}
