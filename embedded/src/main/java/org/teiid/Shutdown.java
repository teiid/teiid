package org.teiid;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Properties;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.metamatrix.common.util.JMXUtil;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;

public class Shutdown {

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Usage:Shutdown <boot-properties-file> <jmxPort>"); //$NON-NLS-1$
			System.exit(-1);
		}
		
		Properties props = loadConfiguration(args[0]);
		
		JMXUtil jmx = new JMXUtil(props.getProperty(DQPEmbeddedProperties.PROCESSNAME));
		
	    JMXServiceURL serviceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://:"+args[1]+"/jmxrmi"); //$NON-NLS-1$ //$NON-NLS-2$
	    JMXConnector connector = JMXConnectorFactory.connect(serviceURL); 
	    MBeanServerConnection mbsc = connector.getMBeanServerConnection(); 

		ServerProxyHandler handler = new ServerProxyHandler(mbsc, jmx.buildName(ServerMBean.TYPE, ServerMBean.NAME));
		Class<?>[] ifaces = { ServerMBean.class };
		ClassLoader tcl = Thread.currentThread().getContextClassLoader();
		ServerMBean server = (ServerMBean) Proxy.newProxyInstance(tcl, ifaces,handler);
		server.shutdown();
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
			props = new Properties();
			props.load(bootProperties);
			
			// enable socket communication by default.
			props.setProperty(DQPEmbeddedProperties.ENABLE_SOCKETS, Boolean.TRUE.toString());
			props.setProperty(DQPEmbeddedProperties.BOOTURL, f.getCanonicalPath());
			props.setProperty(DQPEmbeddedProperties.TEIID_HOME,f.getParentFile().getCanonicalPath());
		} catch (IOException e) {
			System.out.println("Failed to load bootstrap properties file."); //$NON-NLS-1$
			e.printStackTrace();
			System.exit(-3);
		}	
		return props;
	}	

	/**
	 * Taken from JBoss AS Shutdown.java class.
	 */
	private static class ServerProxyHandler implements InvocationHandler {
		ObjectName serverName;
		MBeanServerConnection server;

		ServerProxyHandler(MBeanServerConnection server, ObjectName serverName) {
			this.server = server;
			this.serverName = serverName;
		}

		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			String methodName = method.getName();
			Class[] sigTypes = method.getParameterTypes();
			ArrayList sigStrings = new ArrayList();
			for (int s = 0; s < sigTypes.length; s++) {
				sigStrings.add(sigTypes[s].getName());
			}
			String[] sig = new String[sigTypes.length];
			sigStrings.toArray(sig);
			Object value = null;
			try {
				value = server.invoke(serverName, methodName, args, sig);
			} catch (UndeclaredThrowableException e) {
				throw e.getUndeclaredThrowable();
			}
			return value;
		}
	}

}
