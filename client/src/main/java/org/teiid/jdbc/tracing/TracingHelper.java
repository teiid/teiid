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

package org.teiid.jdbc.tracing;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.teiid.core.util.ReflectionHelper;

/**
 * Used to reflection load logic that is dependent upon the opentracing library, or provide a dummy implementation
 */
public class TracingHelper {

    public interface Injector {
        String getSpanContext();
    }

    private static Logger logger = Logger.getLogger("org.teiid.jdbc"); //$NON-NLS-1$

    private static Injector INJECTOR;

    public static String getSpanContext() {
        if (INJECTOR == null) {
            try {
                INJECTOR = (Injector) ReflectionHelper.create("org.teiid.jdbc.tracing.GlobalTracerInjector", null, TracingHelper.class.getClassLoader()); //$NON-NLS-1$
            } catch (Throwable e) { //must catch both Error and Exception
                logger.log(Level.FINE, "Unable to load opentracing libraries, propagation will not be used", e); //$NON-NLS-1$
            }
            if (INJECTOR == null) {
                INJECTOR = new Injector() {

                    @Override
                    public String getSpanContext() {
                        return null;
                    }
                };
            }
        }
        return INJECTOR.getSpanContext();
    }

}
