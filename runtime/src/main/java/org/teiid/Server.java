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

package org.teiid;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import com.metamatrix.common.config.api.ExtensionModule;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.ApplicationInfo;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.common.util.JMXUtil.FailedToRegisterException;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.ZipFileUtil;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;
import com.metamatrix.dqp.embedded.services.EmbeddedConfigurationService;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.jdbc.EmbeddedConnectionFactoryImpl;

public class Server extends EmbeddedConnectionFactoryImpl implements ServerMBean  {
	
	private Properties props;
	
	public Server(Properties props) {
		this.props = props;
	}
		
	private void start() {
		try {			
			// start the engine.
			initialize(this.props);
			
			getJMXServer().register(TYPE, NAME, this); 
									
		} catch (FailedToRegisterException e) {
			throw new MetaMatrixRuntimeException(e.getCause());
		} catch (MetaMatrixCoreException e) {
			throw new MetaMatrixRuntimeException(e);
		} 
	}
	
	private void stopS(boolean restart) {
 		if (isAlive()) {
			shutdown(restart);
		}
 		
 		try {
			getJMXServer().unregister(TYPE, NAME);
		} catch (FailedToRegisterException e) {
			// ignore
		}
	}
		
	private static boolean duplicateProcess(Properties props) {
		try {
			String teiidHome = props.getProperty(DQPEmbeddedProperties.TEIID_HOME);
			String workDir = props.getProperty(DQPEmbeddedProperties.DQP_WORKDIR, "work"); //$NON-NLS-1$
			String processName = props.getProperty(DQPEmbeddedProperties.PROCESSNAME); 
			
			String parent = new File(teiidHome, workDir).getCanonicalPath();
			File f = new File(parent, "teiid_"+processName+".pid"); //$NON-NLS-1$ //$NON-NLS-2$ 			
			FileChannel channel = new RandomAccessFile(f, "rw").getChannel(); //$NON-NLS-1$
			return (channel.tryLock() == null); 
		} catch (IOException e) {
			// ignore
		}
		return true;
	}	
	
	private static Properties loadConfiguration(String configFile) {
		File f = new File (configFile);
		if (!f.exists()) {
			System.out.println("Missing the bootstrap properties file, failed to start"); //$NON-NLS-1$
			System.exit(-3);			
		}
		
		Properties props = null;
		try {
			FileReader bootProperties = new FileReader(f); 
			props = new Properties(System.getProperties());
			props.load(bootProperties);
			
			// enable socket communication by default.
			props.setProperty(DQPEmbeddedProperties.ENABLE_SOCKETS, Boolean.TRUE.toString());
			props.setProperty(DQPEmbeddedProperties.BOOTURL, f.getCanonicalPath());
			props.setProperty(DQPEmbeddedProperties.TEIID_HOME, System.getProperty(DQPEmbeddedProperties.TEIID_HOME,f.getParentFile().getCanonicalPath()));
			props = PropertiesUtils.resolveNestedProperties(props);
		} catch (IOException e) {
			System.out.println("Failed to load bootstrap properties file."); //$NON-NLS-1$
			e.printStackTrace();
			System.exit(-3);
		}	
		return props;
	}
	
	@Override
	public void shutdown() {
	      new Thread(){
	         public void run(){
	        	 System.out.println("Server being shutdown..."); //$NON-NLS-1$
	        	 stopS(false);	        	 
	         }
	      }.start();		
	}

	@Override
	public void halt() {
	      new Thread() {
	         public void run() {
	            System.err.println("Killing the Teiid Server now!"); //$NON-NLS-1$
	            Runtime.getRuntime().halt(-4);
	         }
	      }.start();
	}
	
	@Override
	public void restart() {
	      new Thread(){
		         public void run(){
		        	 System.out.println("Server being shutdown..."); //$NON-NLS-1$
		        	 stopS(true);	        	 
		         }
		      }.start();		
		
	}
	/**
	 * Start the Server Mode
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("Usage:Server <boot-properties-file>"); //$NON-NLS-1$
			System.exit(-1);
		}
		
		long startTime = System.currentTimeMillis();
		
		// load configuration
		Properties props = loadConfiguration(args[0]);
		
		// check for duplicate process
		String processName = props.getProperty(DQPEmbeddedProperties.PROCESSNAME);
		if (duplicateProcess(props)) {
			System.out.println(DQPEmbeddedPlugin.Util.getString("DQPEmbeddedManager.duplicate_process", processName)); //$NON-NLS-1$
			System.exit(-2);			
		}
		
		// load the server
		Server s = new Server(props);
		s.start();
		
		String port = props.getProperty(DQPEmbeddedProperties.SERVER_PORT);
		long time = System.currentTimeMillis() - startTime;
		
		System.out.println("Teiid Server started on port = "+ port + " in "+time/1000+" Secs"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
		
		if (LogManager.isMessageToBeRecorded(LogConstants.CTX_PROCESS_INFO_LOG, MessageLevel.INFO)) {
			s.logProcessInfo();
		}
		
		// wait.
		while(s.isAlive()) {
	        try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				break;
			}				
		}

		// if for some reason engine is still alive kill it.
		if (s.isAlive()) {
			s.stopS(false);
		}

		// exit code to restart the process.
		if (s.shouldRestart()) {
			System.exit(10);
		}
	}
	
    public void logProcessInfo() {
        ApplicationInfo info = ApplicationInfo.getInstance();
        StringBuffer sb = new StringBuffer("Process Information"); //$NON-NLS-1$

        sb.append('\n');
        sb.append(" Process Name:               " + props.getProperty(DQPEmbeddedProperties.PROCESSNAME) ); //$NON-NLS-1$
        sb.append('\n');
        sb.append(" Hostname:              " + getAddress().getCanonicalHostName() ); //$NON-NLS-1$
        sb.append('\n');
        sb.append(" Version:               ").append(ApplicationInfo.getInstance().getReleaseNumber()); //$NON-NLS-1$
        sb.append('\n');
        sb.append(" Build Date:            ").append(ApplicationInfo.getInstance().getBuildDate()); //$NON-NLS-1$
                        
        sb.append("\n---- System Properties ----\n");  //$NON-NLS-1$             
        sb.append(PropertiesUtils.prettyPrint(System.getProperties()));

        sb.append("\n---- Deploy Properties ----\n");  //$NON-NLS-1$                             
        sb.append(PropertiesUtils.prettyPrint(props));
                    
        sb.append("\n# of Processors: " + java.lang.Runtime.getRuntime().availableProcessors());//$NON-NLS-1$  
        sb.append("\nMax Avail memory: " + java.lang.Runtime.getRuntime().maxMemory());//$NON-NLS-1$               
        sb.append("\nFree memory: " + java.lang.Runtime.getRuntime().freeMemory());//$NON-NLS-1$  
        
        sb.append(info.getClasspathInfo());
        
        sb.append("\n\n---- Extension Jars Manifest Info ----\n");  //$NON-NLS-1$             

        logManifestInfoForExtensionModules(sb);
        
        LogManager.logInfo(LogConstants.CTX_PROCESS_INFO_LOG, sb.toString());
    }
    
    private void logManifestInfoForExtensionModules(StringBuffer sb) {
        try {
        	EmbeddedConfigurationService ecs = (EmbeddedConfigurationService)getDQP().getEnvironment().findService(DQPServiceNames.CONFIGURATION_SERVICE);
            for (ExtensionModule module : ecs.getExtensionModules()) {
                sb.append("\n == Jar: " + module.getFullName() + " ===== " );  //$NON-NLS-1$ //$NON-NLS-2$  
                
                File tempArchive = File.createTempFile("teiid", ".jar"); //$NON-NLS-1$ //$NON-NLS-2$
                tempArchive.deleteOnExit();
            	FileUtils.write(module.getFileContents(), tempArchive);
                
                Manifest m = ZipFileUtil.getManifest(tempArchive);
                tempArchive.delete();
                if (m != null) {
                    // only print the manifest info for MetaMatrix related jars that have
                    // the product information section
                    Attributes manifestAttributes = m.getMainAttributes();
                    if(manifestAttributes == null || manifestAttributes.isEmpty()){
                        continue;
                        
                    } 
                                                
                    for (Iterator ita = manifestAttributes.keySet().iterator(); ita.hasNext();) {
                        Object n =  ita.next();
                        Object v = manifestAttributes.get(n);
                        
                        sb.append("\n");//$NON-NLS-1$
                        sb.append("   ");//$NON-NLS-1$
                        sb.append(n.toString());
                        sb.append(":        "); //$NON-NLS-1$
                        sb.append(v.toString());
                        
                    }
                    sb.append("\n");//$NON-NLS-1$
                    
                } 
            }
        } catch(Exception e) {
            sb.append("**** Error: Unable to list manifest - msg: " + e.getMessage());//$NON-NLS-1$
        } 
        
    }

}
