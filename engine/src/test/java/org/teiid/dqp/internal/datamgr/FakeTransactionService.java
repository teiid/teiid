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

package org.teiid.dqp.internal.datamgr;

import javax.resource.spi.XATerminator;
import javax.transaction.TransactionManager;

import org.teiid.common.queue.FakeWorkManager;
import org.teiid.core.util.SimpleMock;
import org.teiid.dqp.internal.process.TransactionServerImpl;


public class FakeTransactionService extends TransactionServerImpl {

	public FakeTransactionService() {
		this.setTransactionManager(SimpleMock.createSimpleMock(TransactionManager.class));
		this.setXaTerminator(SimpleMock.createSimpleMock(XATerminator.class));
		this.setWorkManager(new FakeWorkManager());
	}
	
}
