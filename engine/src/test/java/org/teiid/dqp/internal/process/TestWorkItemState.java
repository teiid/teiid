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

package org.teiid.dqp.internal.process;

import static org.junit.Assert.*;

import org.junit.Test;


public class TestWorkItemState {
	
	private class TestWorkItem extends AbstractWorkItem {

		private boolean isDone;
		private boolean callMoreWork;
		private boolean resumed;
		
		private TestWorkItem() {
			this(false, false);
		}
		
		private TestWorkItem(boolean done, boolean callMoreWork) {
			super(false);
			this.isDone = done;
			this.callMoreWork = callMoreWork;
		}
		
		@Override
		protected boolean isDoneProcessing() {
			return isDone;
		}

		@Override
		protected void process() {
			assertWorkingState();
			if (callMoreWork) {
				this.moreWork();
			}
		}

		@Override
		protected void resumeProcessing() {
			this.resumed = true;
		}

		@Override
		public String toString() {
			return "TestItem"; //$NON-NLS-1$
		}
		
	    private void checkState(ThreadState expectedState) {
	        assertEquals(expectedState, getThreadState());
	    }
	    
	    private void assertIdleState() {
	        checkState(ThreadState.IDLE);
	    }
	       
	    private void assertMoreWorkState() {
	        checkState(ThreadState.MORE_WORK);
	    }
	    
	    private void assertWorkingState() {
	        checkState(ThreadState.WORKING);
	    }
	    
	    private void assertDoneState() {
	        checkState(ThreadState.DONE);
	    }
	}

    @Test public void testInitialState() {
        TestWorkItem item = new TestWorkItem();
        item.assertMoreWorkState();
    }
    
    @Test public void testGotoIdleState() {
        TestWorkItem item = new TestWorkItem();
        item.run();
        item.assertIdleState();
    }
    
    @Test public void testGotoMoreWorkState() {
    	TestWorkItem item = new TestWorkItem();
        item.run();
        item.moreWork();
        item.assertMoreWorkState();
    }
    
    @Test public void testGotoWorkingState() {
    	TestWorkItem item = new TestWorkItem();
    	item.run();
    	item.moreWork();
    	item.run();
    }
    
    @Test public void testResume() {
    	TestWorkItem item = new TestWorkItem();
    	item.run();
    	assertFalse(item.resumed);
    	item.moreWork();
    	assertTrue(item.resumed);
    }
    
    @Test public void testResumeDuringWorking() {
    	TestWorkItem item = new TestWorkItem(false, true);
    	assertFalse(item.resumed);
    	item.run();
    	assertTrue(item.resumed);
    }
    
    @Test public void testRunAfterDone() {
    	TestWorkItem item = new TestWorkItem(true, false);
    	item.run();
    	item.assertDoneState();
    	try {
    		item.run();
    		fail("exception expected"); //$NON-NLS-1$
    	} catch (IllegalStateException e) {
    		
    	}
    }
    
    @Test public void testRunDuringIdle() {
    	TestWorkItem item = new TestWorkItem();
    	item.run();
    	item.assertIdleState();
    	try {
    		item.run();
    		fail("exception expected"); //$NON-NLS-1$
    	} catch (IllegalStateException e) {
    		
    	}
    }
        
}
