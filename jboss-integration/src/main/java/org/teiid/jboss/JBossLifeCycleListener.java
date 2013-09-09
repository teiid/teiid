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
package org.teiid.jboss;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jboss.as.controller.ControlledProcessState;
import org.jboss.as.controller.ControlledProcessStateService;
import org.jboss.msc.service.AbstractServiceListener;
import org.jboss.msc.service.ServiceContainer.TerminateListener;
import org.jboss.msc.service.ServiceController;
import org.teiid.deployers.ContainerLifeCycleListener;

class JBossLifeCycleListener extends AbstractServiceListener<Object> implements TerminateListener, ContainerLifeCycleListener {
	private boolean shutdownInProgress = false;
	private List<ContainerLifeCycleListener.LifeCycleEventListener> listeners = Collections.synchronizedList(new ArrayList<ContainerLifeCycleListener.LifeCycleEventListener>());
	private ControlledProcessStateService state;
	
	@Override
	public boolean isShutdownInProgress() {
		return shutdownInProgress;
	}
	
	@Override
	public boolean isBootInProgress() {
		return state.getCurrentState().equals(ControlledProcessState.State.STARTING);
	}

	@Override
	public void handleTermination(Info info) {
		if (info.getShutdownInitiated() > 0) {
			this.shutdownInProgress = true;
		}
	}

	@Override
	public void addListener(LifeCycleEventListener listener) {
		listeners.add(listener);
	}
	
	@Override
    public void transition(final ServiceController controller, final ServiceController.Transition transition) {
		if (transition.equals(ServiceController.Transition.UP_to_STOP_REQUESTED)) {
			this.shutdownInProgress = true;
		}
		else if (transition.equals(ServiceController.Transition.STOP_REQUESTED_to_UP)) {
			this.shutdownInProgress = false;
		}
    }	
	
	void setControlledProcessStateService(ControlledProcessStateService state) {
		this.state = state;	
	}
}
