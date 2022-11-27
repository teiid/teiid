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

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.client.SourceWarning;
import org.teiid.core.CorePlugin;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.ExternalizeUtil;
import org.teiid.core.util.ObjectInputStreamWithClassloader;
import org.teiid.core.util.ReflectionHelper;


public class ExceptionHolder implements Externalizable {

    private Throwable exception;
    private boolean nested = false;

    public ExceptionHolder() {
    }

    public ExceptionHolder(Throwable exception) {
        this.exception = exception;
    }

    public ExceptionHolder(Throwable exception, boolean nested) {
        this.exception = exception;
        this.nested = nested;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        List<String> classNames = ExternalizeUtil.readList(in, String.class);
        String message = (String)in.readObject();
        StackTraceElement[] stackTrace = (StackTraceElement[])in.readObject();
        String code = (String)in.readObject();
        ExceptionHolder causeHolder = (ExceptionHolder)in.readObject();
        byte[] serializedException = (byte[])in.readObject();

        this.exception = readFromByteArray(serializedException);

        if (this.exception == null) {
            Throwable t = buildException(classNames, message, stackTrace, code);
            if (causeHolder != null) {
                t.initCause(causeHolder.exception);
            }
            this.exception = t;

            if (this.exception instanceof SQLException) {
                try {
                    int count = in.readInt();
                    for (int i = 0; i < count; i++) {
                        ExceptionHolder next = (ExceptionHolder)in.readObject();
                        if (next.exception instanceof SQLException) {
                            ((SQLException)this.exception).setNextException((SQLException) next.exception);
                        }
                    }
                } catch (EOFException e) {

                } catch (OptionalDataException e) {

                }
            } else if (!classNames.isEmpty() && classNames.get(0).equals(SourceWarning.class.getName())) {
                try {
                    String connectorBindingName = in.readUTF();
                    String modelName = in.readUTF();
                    boolean partial = in.readBoolean();
                    this.exception = new SourceWarning(modelName, connectorBindingName, this.exception.getCause(), partial);
                } catch (EOFException e) {
                    //for backwards compatibility
                } catch (OptionalDataException e) {
                    //for backwards compatibility
                }
            }
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        List<String> classNames = new ArrayList<String>();
        Class<?> clazz = exception.getClass();
        while (clazz != null) {
            if (clazz == Throwable.class || clazz == Exception.class) {
                break;
            }
            classNames.add(clazz.getName());
            clazz = clazz.getSuperclass();
        }
        ExternalizeUtil.writeList(out, classNames);
        out.writeObject(exception.getMessage());
        out.writeObject(exception.getStackTrace());
        if (exception instanceof TeiidException) {
            out.writeObject(((TeiidException)exception).getCode());
        } else {
            out.writeObject(null);
        }

        // specify that this cause is nested exception; not top level
        if (this.exception.getCause() != null && this.exception.getCause() != this.exception) {
            out.writeObject(new ExceptionHolder(this.exception.getCause(), true));
        }
        else {
            out.writeObject(null);
        }

        // only for the top level exception write the serialized block for the object
        if (!nested) {
            out.writeObject(writeAsByteArray(this.exception));
        }
        else {
            out.writeObject(null);
        }
        // handle SQLException chains
        if (exception instanceof SQLException) {
            if (nested) {
                out.writeInt(0);
            } else {
                SQLException se = (SQLException)exception;
                SQLException next = se.getNextException();
                int count = 0;
                while (next != null) {
                    count++;
                    next = next.getNextException();
                }
                out.writeInt(count);
                next = se.getNextException();
                while (next != null) {
                    out.writeObject(new ExceptionHolder(next, true));
                    next = next.getNextException();
                }
            }
        } else if (exception instanceof SourceWarning) {
            SourceWarning sw = (SourceWarning)exception;
            out.writeUTF(sw.getConnectorBindingName());
            out.writeUTF(sw.getModelName());
            out.writeBoolean(sw.isPartialResultsError());
        }
    }

    public Throwable getException() {
        return exception;
    }

    private Throwable buildException(List<String> classNames, String message, StackTraceElement[] stackTrace, String code) {
        String originalClass = Exception.class.getName();

        if (!classNames.isEmpty()) {
            originalClass = classNames.get(0);
        }

        List<String> args = Arrays.asList(CorePlugin.Util.getString("ExceptionHolder.converted_exception", message, originalClass)); //$NON-NLS-1$

        Throwable result = null;
        for (String className : classNames) {
            try {
                result = (Throwable)ReflectionHelper.create(className, args, ExceptionHolder.class.getClassLoader());
                break;
            } catch (TeiidException e1) {
                //
            }
        }

        if (result == null) {
            result = new TeiidRuntimeException(args.get(0));
        } else if (result instanceof TeiidException) {
            ((TeiidException)result).setCode(code);
            ((TeiidException)result).setOriginalType(classNames.get(0));
        }

        result.setStackTrace(stackTrace);
        return result;
    }

    private byte[] writeAsByteArray(Throwable t) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(t);
            oos.flush();
            oos.close();
            return baos.toByteArray();
        } catch (IOException e) {
            return null;
        }
    }

    private Throwable readFromByteArray(byte[] contents) {
        // only for top level we would have the contents as not null.
        if (contents != null) {
            ByteArrayInputStream bais = new ByteArrayInputStream(contents);
            try {
                ObjectInputStream ois = new ObjectInputStreamWithClassloader(bais, ExceptionHolder.class.getClassLoader());
                return (Throwable)ois.readObject();
            } catch (Exception e) {
                //
            }
        }
        return null;
    }

    public static List<ExceptionHolder> toExceptionHolders(List<? extends Throwable> throwables){
        List<ExceptionHolder> list = new ArrayList<ExceptionHolder>();
        for (Throwable t: throwables) {
            list.add(new ExceptionHolder(t));
        }
        return list;
    }

    public static List<Throwable> toThrowables(List<ExceptionHolder> exceptionHolders) {
        List<Throwable> list = new ArrayList<Throwable>();
        for(ExceptionHolder e: exceptionHolders) {
            list.add(e.getException());
        }
        return list;
    }

}
