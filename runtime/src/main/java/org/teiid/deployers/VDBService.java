package org.teiid.deployers;

import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.teiid.adminapi.impl.VDBMetaData;

public class VDBService implements Service<VDBMetaData> {
	private VDBMetaData vdb;
	
	public VDBService(VDBMetaData metadata) {
		this.vdb = metadata;
	}
	@Override
	public void start(StartContext context) throws StartException {
		// rameshTODO Auto-generated method stub
		
	}

	@Override
	public void stop(StopContext context) {
		// rameshTODO Auto-generated method stub
		
	}

	@Override
	public VDBMetaData getValue() throws IllegalStateException,IllegalArgumentException {
		return this.vdb;
	}

}
