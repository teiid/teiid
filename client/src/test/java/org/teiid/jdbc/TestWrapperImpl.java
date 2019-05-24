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

package org.teiid.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.sql.Wrapper;

import org.teiid.jdbc.WrapperImpl;

import junit.framework.TestCase;
@SuppressWarnings("nls")
public class TestWrapperImpl extends TestCase {

    interface Foo extends Wrapper {
        void callMe();
    }

    static class FooImpl extends WrapperImpl implements Foo {

        boolean wasCalled;

        public void callMe() {
            wasCalled = true;
        }

    }

    public void testProxy() throws SQLException {

        final FooImpl fooImpl = new FooImpl();

        Foo proxy = (Foo)Proxy.newProxyInstance(TestWrapperImpl.class.getClassLoader(), new Class[] {Foo.class}, new InvocationHandler() {

            public Object invoke(Object arg0, Method arg1, Object[] arg2)
                    throws Throwable {
                if (arg1.getName().equals("callMe")) {
                    return null;
                }
                try {
                    return arg1.invoke(fooImpl, arg2);
                } catch (InvocationTargetException e) {
                    throw e.getTargetException();
                }
            }

        });

        proxy.callMe();

        assertFalse(fooImpl.wasCalled);

        proxy.unwrap(Foo.class).callMe();

        assertTrue(fooImpl.wasCalled);

        try {
            proxy.unwrap(String.class);
            fail("expected exception");
        } catch (SQLException e) {
            assertEquals("Wrapped object is not an instance of class java.lang.String", e.getMessage());
        }
    }

}
