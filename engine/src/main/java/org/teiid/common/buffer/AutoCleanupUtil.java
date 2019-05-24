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

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;

public class AutoCleanupUtil {

    public interface Removable {
        public void remove();
    }

    static final class PhantomCleanupReference extends PhantomReference<Object> {

        private Removable removable;

        public PhantomCleanupReference(Object referent, Removable removable) {
            super(referent, QUEUE);
            this.removable = removable;
        }

        public void cleanup() {
            try {
                this.removable.remove();
            } finally {
                this.removable = null;
                this.clear();
            }
        }
    }

    private static ReferenceQueue<Object> QUEUE = new ReferenceQueue<Object>();
    private static final Set<PhantomReference<Object>> REFERENCES = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<PhantomReference<Object>, Boolean>()));

    public static PhantomReference<Object> setCleanupReference(Object o, Removable r) {
        PhantomCleanupReference ref = new PhantomCleanupReference(o, r);
        REFERENCES.add(ref);
        doCleanup(true);
        return ref;
    }

    public static void removeCleanupReference(PhantomReference<Object> ref) {
        if (ref == null) {
            return;
        }
        REFERENCES.remove(ref);
        ref.clear();
    }

    public static void doCleanup(boolean quick) {
        int max = quick?10:Integer.MAX_VALUE;
        for (int i = 0; i < max; i++) {
            PhantomCleanupReference ref = (PhantomCleanupReference)QUEUE.poll();
            if (ref == null) {
                break;
            }
            try {
                ref.cleanup();
            } catch (Throwable e) {
                LogManager.logWarning(LogConstants.CTX_DQP, e, "Error cleaning up."); //$NON-NLS-1$
            }
            REFERENCES.remove(ref);
        }
    }
}
