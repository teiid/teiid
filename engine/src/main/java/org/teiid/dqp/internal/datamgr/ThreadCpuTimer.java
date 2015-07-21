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

package org.teiid.dqp.internal.datamgr;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;

/**
 * Timer class that uses the ThreadMXBean for CPU timing
 */
public class ThreadCpuTimer {
	
	private static ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
	private int totalTime = -1;
	private long lastTime = -1;
	private boolean active;
	
	public ThreadCpuTimer() {
		active = threadMXBean.isThreadCpuTimeSupported() 
				&& threadMXBean.isThreadCpuTimeEnabled()
				&& LogManager.isMessageToBeRecorded(LogConstants.CTX_COMMANDLOGGING, MessageLevel.DETAIL);
	}
	
	public void start() {
		if (!active) {
			return;
		}
		try {
			lastTime = threadMXBean.getCurrentThreadCpuTime();
		} catch (UnsupportedOperationException e) {
			inactivate();
		}
		
	}

	private void inactivate() {
		active = false;
		lastTime = -1;
		totalTime = -1;
	}
	
	public long stop() {
		if (!active) {
			return -1;
		}
		if (lastTime != -1) {
			try {
				long time = threadMXBean.getCurrentThreadCpuTime();
				if (time != -1) {
					if (totalTime == -1) {
						totalTime = 0;
					}
					totalTime += (time - lastTime);
				} else {
					inactivate();
				}
			} catch (UnsupportedOperationException e) {
				inactivate();
			}
		}
		lastTime = -1;
		return totalTime;
	}

}
