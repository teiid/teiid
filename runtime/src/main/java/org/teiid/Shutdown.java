package org.teiid;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
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
		
		Properties props = Server.loadConfiguration(args[0]);
		
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
			Class<?>[] sigTypes = method.getParameterTypes();
			String[] sig = new String[sigTypes.length];
			for (int s = 0; s < sigTypes.length; s++) {
				sig[s] = sigTypes[s].getName();
			}
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
