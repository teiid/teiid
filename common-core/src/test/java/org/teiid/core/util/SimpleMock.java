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

package org.teiid.core.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class SimpleMock extends MixinProxy {

    SimpleMock(Object[] baseInstances) {
        super(baseInstances);
    }

    @Override
    protected Object noSuchMethodFound(Object proxy, Method method,
            Object[] args) throws Throwable {
        Class clazz = method.getReturnType();

        if (clazz == Void.TYPE) {
            return null;
        }

        if (clazz.isPrimitive()) {
            if (clazz == boolean.class) {
                return false;
            }
            return 0;
        }

        if (!clazz.isInterface()) {
            try {
                Constructor c = clazz.getDeclaredConstructor(new Class[] {});
                if (c != null) {
                    try {
                        return c.newInstance(new Object[] {});
                    } catch (InvocationTargetException e) {
                        throw e.getTargetException();
                    }
                }
            } catch (NoSuchMethodException e) {
            }

            return null;
        }

        Class[] interfaces = clazz.getInterfaces();

        if (clazz.isInterface()) {
            interfaces = new Class[] {clazz};
        }

        if (interfaces != null && interfaces.length > 0) {
            return Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), interfaces, this);
        }

        return null;
    }

    public static <T> T createSimpleMock(Class<T> clazz) {
        return (T)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {clazz}, new SimpleMock(new Object[] {}));
    }

    public static <T> T createSimpleMock(Object baseInstance, Class<T> clazz) {
        if (baseInstance instanceof Object[]) {
            return (T)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {clazz}, new SimpleMock((Object[])baseInstance));
        }
        return (T)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {clazz}, new SimpleMock(new Object[] {baseInstance}));
    }

    public static Object createSimpleMock(Object[] baseInstances, Class[] interfaces) {
        return Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), interfaces, new SimpleMock(baseInstances));
    }

}
