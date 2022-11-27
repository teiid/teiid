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

package org.teiid.logging;


/**
 * LogListener
 */
public interface Logger {

    /**
     * Is the logging for the given context at the specified message level enabled.
     * @param context
     * @param msgLevel
     * @return
     */
    boolean isEnabled(String context, int msgLevel);

    void log(int level, String context, Object... msg);

    void log(int level, String context, Throwable t, Object... msg);

    /**
     * Shut down this listener, requesting it clean up and release any resources it
     * may have acquired during its use.  The listener is free to ignore this
     * request if it is not responsible for managing the resources it uses or if
     * there are no resources.
     */
    void shutdown();

    void putMdc(String key, String val);

    void removeMdc(String key);

}
