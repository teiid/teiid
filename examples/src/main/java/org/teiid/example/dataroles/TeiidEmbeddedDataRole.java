package org.teiid.example.dataroles;

import java.io.ByteArrayInputStream;

import org.teiid.example.ExampleBase;
import org.teiid.example.util.FileUtils;
import org.teiid.resource.adapter.file.FileManagedConnectionFactory;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.file.FileExecutionFactory;

public class TeiidEmbeddedDataRole extends ExampleBase{
	
	@Override
	public void execute(String vdb) throws Exception {
		
		server = new EmbeddedServer();
		
		FileExecutionFactory fileExecutionFactory = new FileExecutionFactory();
    	fileExecutionFactory.start();
    	server.addTranslator("file", fileExecutionFactory);
    	
    	FileManagedConnectionFactory managedconnectionFactory = new FileManagedConnectionFactory();
		managedconnectionFactory.setParentDirectory(FileUtils.readFilePath("embedded-portfolio", "data"));
		server.addConnectionFactory("java:/marketdata-file", managedconnectionFactory.createConnectionFactory());
		
		start(false);
		
		server.deployVDB(new ByteArrayInputStream(vdb.getBytes()));
		
		conn = server.getDriver().connect("jdbc:teiid:Portfolio", null);
		
		tearDown();
	}

	public static void main(String[] args) throws Exception {
		new TeiidEmbeddedDataRole().execute(FileUtils.readFileContent("dataroles", "portfolio-vdb.xml"));
	}

	

}
