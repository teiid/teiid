package org.teiid.translator.object.infinispan;


import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectCacheConnection;
import org.teiid.translator.object.ObjectExecutionFactory;
import org.teiid.translator.object.ObjectSourceProxy;

@Translator(name="infinispanRemote", description="The Execution Factory for Remote Infinispan Cache")
public class InfinispanRemoteExecutionFactory extends ObjectExecutionFactory {
	
	public InfinispanRemoteExecutionFactory() {
		super();
	}

	@Override
	public void start() throws TranslatorException {
		super.start();
  	
	}


	protected ObjectSourceProxy createProxy(ObjectCacheConnection connection)
			throws TranslatorException {
		return new InfinispanProxy(connection, this);
	}	
    
    
}
