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

import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.deployers.VDBStatusChecker;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;

class TranslatorService implements Service<VDBTranslatorMetaData> {
	private VDBTranslatorMetaData translator;
	
	final InjectedValue<TranslatorRepository> repositoryInjector = new InjectedValue<TranslatorRepository>();
	final InjectedValue<VDBStatusChecker> statusCheckerInjector = new InjectedValue<VDBStatusChecker>();
		
	public TranslatorService(VDBTranslatorMetaData translator) {
		this.translator = translator;
	}
	
	@Override
	public void start(StartContext context) throws StartException {
		this.repositoryInjector.getValue().addTranslatorMetadata(this.translator.getName(), this.translator);
		this.statusCheckerInjector.getValue().translatorAdded(this.translator.getName());
	}

	@Override
	public void stop(StopContext context) {
		this.repositoryInjector.getValue().removeTranslatorMetadata(this.translator.getName());
		this.statusCheckerInjector.getValue().translatorRemoved(this.translator.getName());
		LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50010, this.translator.getName()));
	}

	@Override
	public VDBTranslatorMetaData getValue() throws IllegalStateException, IllegalArgumentException {
		return this.translator;
	}
}
