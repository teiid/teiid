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

package org.teiid.client.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import org.teiid.client.SourceWarning;
import org.teiid.client.xa.XATransactionException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;


public class ExceptionUtil {

    public static <T extends Throwable> T getExceptionOfType(Throwable ex, Class<T> cls) {
        while (ex != null) {
            if (cls.isAssignableFrom(ex.getClass())) {
                return cls.cast(ex);
            }
            if (ex.getCause() == ex) {
                break;
            }
            ex = ex.getCause();
        }
        return null;
    }

    public static Throwable convertException(Method method, Throwable exception) {
        boolean canThrowXATransactionException = false;
        boolean canThrowComponentException = false;
        Class<?>[] exceptionClasses = method.getExceptionTypes();
        for (int i = 0; i < exceptionClasses.length; i++) {
            if (exceptionClasses[i].isAssignableFrom(exception.getClass())) {
                return exception;
            }
            if (!canThrowComponentException) {
                canThrowComponentException = TeiidComponentException.class.isAssignableFrom(exceptionClasses[i]);
            }
            if (!canThrowXATransactionException) {
                canThrowXATransactionException = XATransactionException.class.isAssignableFrom(exceptionClasses[i]);
            }
        }
        if (canThrowComponentException) {
            return new TeiidComponentException(exception);
        }
        if (canThrowXATransactionException) {
            return new XATransactionException(exception);
        }
        if (RuntimeException.class.isAssignableFrom(exception.getClass())) {
            return exception;
        }
        return new TeiidRuntimeException(exception);
    }

    /**
     * Strip out the message and optionally the stacktrace
     * @param t
     * @return
     */
    public static Throwable sanitize(Throwable t, boolean perserveStack) {
        String code = null;
        if (t instanceof TeiidException) {
            code = ((TeiidException)t).getCode();
        } else if (t instanceof TeiidRuntimeException) {
            code = ((TeiidRuntimeException)t).getCode();
        } else {
            code = t.getClass().getName();
        }
        Throwable child = null;
        if (t.getCause() != null && t.getCause() != t) {
            child = sanitize(t.getCause(), perserveStack);
        }
        Class<?> clazz = t.getClass();
        Throwable result = null;
        while (clazz != null) {
            if (clazz == Throwable.class || clazz == Exception.class) {
                break;
            }
            Constructor<?> ctor = null;
            try {
                ctor = clazz.getDeclaredConstructor(new Class<?>[] {String.class});
                result = (Throwable) ctor.newInstance(code);
                break;
            } catch (Exception e) {

            }
            clazz = clazz.getSuperclass();
        }
        if (result == null) {
            result = new TeiidException(code);
        }
        if (result instanceof TeiidException) {
            ((TeiidException)result).setCode(code);
        } else if (result instanceof TeiidRuntimeException) {
            ((TeiidException)result).setCode(code);
        }
        if (child != null) {
            result.initCause(child);
        }
        if (perserveStack) {
            result.setStackTrace(t.getStackTrace());
        } else {
            result.setStackTrace(SourceWarning.EMPTY_STACK_TRACE);
        }
        return result;
    }
}
