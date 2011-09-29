/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.common.buffer;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

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

	public static void setCleanupReference(Object o, Removable r) {
		REFERENCES.add(new PhantomCleanupReference(o, r));
		doCleanup();
	}

	public static void doCleanup() {
		for (int i = 0; i < 10; i++) {
			PhantomCleanupReference ref = (PhantomCleanupReference)QUEUE.poll();
			if (ref == null) {
				break;
			}
			ref.cleanup();
			REFERENCES.remove(ref);
		}
	}
}
