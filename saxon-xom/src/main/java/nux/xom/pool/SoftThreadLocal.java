/*
 * Copyright (c) 2005, The Regents of the University of California, through
 * Lawrence Berkeley National Laboratory (subject to receipt of any required
 * approvals from the U.S. Dept. of Energy). All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * (1) Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 
 * (2) Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * 
 * (3) Neither the name of the University of California, Lawrence Berkeley
 * National Laboratory, U.S. Dept. of Energy nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * You are under no obligation whatsoever to provide any bug fixes, patches, or
 * upgrades to the features, functionality or performance of the source code
 * ("Enhancements") to anyone; however, if you choose to make your Enhancements
 * available either publicly, or directly to Lawrence Berkeley National
 * Laboratory, without imposing a separate written license agreement for such
 * Enhancements, then you hereby grant the following license: a non-exclusive,
 * royalty-free perpetual license to install, use, modify, prepare derivative
 * works, incorporate into other computer software, distribute, and sublicense
 * such enhancements or derivative works thereof, in binary and source code
 * form.
 */
package nux.xom.pool;

import java.lang.ref.SoftReference;

/**
 * ThreadLocal utility that allows garbage collection of its value via a {@link SoftReference}.
 * Typically used for static vars such as
 * 
 * <pre>
 * private static final ThreadLocal xyz = new SoftThreadLocal() { ... };
 * </pre>
 * 
 * To use it, override method initialSoftValue() instead of initialValue().
 * <p>
 * See http://java.sun.com/docs/hotspot/PerformanceFAQ.html#175 and 
 * http://java.sun.com/docs/hotspot/gc5.0/gc_tuning_5.html and
 * http://www.theserverside.com/news/thread.tss?thread_id=29865 and
 * http://www.theserverside.com/discussions/thread.tss?thread_id=26023
 * 
 * @author whoschek.AT.lbl.DOT.gov
 * @author $Author: hoschek3 $
 * @version $Revision: 1.9 $, $Date: 2005/11/30 05:12:20 $
 */
abstract class SoftThreadLocal extends ThreadLocal {

	/** Override this method instead of initialValue() */
	protected abstract Object initialSoftValue();
	
	protected final Object initialValue() { // lazy init
		return wrap(initialSoftValue());
	}

	public Object get() {
		Object value = ((SoftReference) super.get()).get(); // unwrap
		if (value == null) { // reinitialize if it's been silently garbage collected
			value = initialSoftValue();
			set(value);
		}
		return value;
	}
	    
	public void set(Object value) {
		super.set(wrap(value));
	}
	
	private static SoftReference wrap(Object value) {
		return new SoftReference(value);
	}
		
}
