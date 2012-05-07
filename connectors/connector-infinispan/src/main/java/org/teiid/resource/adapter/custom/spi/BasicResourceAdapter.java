/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.teiid.resource.adapter.custom.spi;

import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterInternalException;
import javax.resource.spi.XATerminator;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.WorkManager;
import javax.transaction.xa.XAResource;

public class BasicResourceAdapter implements ResourceAdapter {
	BootstrapContext ctx;
	
	@Override
	public void endpointActivation(MessageEndpointFactory endpointFactory, ActivationSpec spec) throws ResourceException {
		throw new UnsupportedOperationException("not supported"); //$NON-NLS-1$
	}

	@Override
	public void endpointDeactivation(MessageEndpointFactory endpointFactory, ActivationSpec spec) {
		throw new UnsupportedOperationException("not supported"); //$NON-NLS-1$
	}

	@Override
	public XAResource[] getXAResources(ActivationSpec[] specs) throws ResourceException {
		return new XAResource[0];
	}

	@Override
	public void start(BootstrapContext ctx) throws ResourceAdapterInternalException {
		this.ctx = ctx;
	}

	@Override
	public void stop() {
	}

	public WorkManager getWorkManager() {
		return ctx.getWorkManager();
	}
	
	public XATerminator getXATerminator() {
		return ctx.getXATerminator();
	}
}
