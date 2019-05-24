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

package org.teiid.common.buffer;

import java.util.Arrays;

import org.teiid.core.TeiidComponentException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;

/**
 * This exception is thrown if the buffer manager blocks waiting on input during
 * processing.  This is an indication that more data will be available, but is
 * not currently available.
 */
public class BlockedException extends TeiidComponentException {

    public static final BlockedException INSTANCE = new BlockedException();

    public static final BlockedException BLOCKED_ON_MEMORY_EXCEPTION = new BlockedException();

    /**
     * No-arg costructor required by Externalizable semantics
     */
    public BlockedException() {
        super();
    }

    public static BlockedException block(Object... msg) {
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, msg);
        }
        return INSTANCE;
    }

    public static BlockedException blockWithTrace(Object... msg) {
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
            BlockedException be = new BlockedException();
            if (be.getStackTrace().length > 0) {
                be.setStackTrace(Arrays.copyOfRange(be.getStackTrace(), 1, Math.max(0, Math.min(8, be.getStackTrace().length))));
            }
            LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, be, msg);
        }
        return INSTANCE;
    }

}
