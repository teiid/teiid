package org.teiid.example.dataroles;

import java.io.ByteArrayInputStream;
import java.util.concurrent.ArrayBlockingQueue;

import org.teiid.example.ExampleBase;
import org.teiid.example.util.FileUtils;
import org.teiid.resource.adapter.file.FileManagedConnectionFactory;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.file.FileExecutionFactory;

public class TeiidEmbeddedDataRole extends ExampleBase{
	
    public void execute(String vdb) throws Exception {
        execute(vdb, null);
    }
    
	@Override
	public void execute(String vdb, ArrayBlockingQueue<String> queue) throws Exception {
		
		server = new EmbeddedServer();
		
		FileExecutionFactory fileExecutionFactory = new FileExecutionFactory();
    	fileExecutionFactory.start();
    	server.addTranslator("file", fileExecutionFactory); //$NON-NLS-1$ 
    	
    	FileManagedConnectionFactory managedconnectionFactory = new FileManagedConnectionFactory();
		managedconnectionFactory.setParentDirectory(FileUtils.readFilePath("embedded-portfolio", "data")); //$NON-NLS-1$  //$NON-NLS-2$ 
		server.addConnectionFactory("java:/marketdata-file", managedconnectionFactory.createConnectionFactory()); //$NON-NLS-1$  
		
		start(false);
		
		server.deployVDB(new ByteArrayInputStream(vdb.getBytes()));
		
		conn = server.getDriver().connect("jdbc:teiid:Portfolio", null); //$NON-NLS-1$ 
		
		tearDown();
		
		add(queue, "Exit"); //$NON-NLS-1$
	}

	public static void main(String[] args) throws Exception {
		new TeiidEmbeddedDataRole().execute(FileUtils.readFileContent("dataroles", "portfolio-vdb.xml")); //$NON-NLS-1$  //$NON-NLS-2$ 
	}

	

}
