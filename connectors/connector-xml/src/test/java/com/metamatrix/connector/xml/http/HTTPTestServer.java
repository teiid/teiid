package com.metamatrix.connector.xml.http;

import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.DefaultHandler;
import org.mortbay.jetty.handler.HandlerList;
import org.mortbay.jetty.handler.ResourceHandler;
import org.mortbay.jetty.servlet.Context;

import com.metamatrix.connector.xml.base.ProxyObjectFactory;

public class HTTPTestServer {

	private static Server server;
	
	public HTTPTestServer() {
		server = initServer();

        try {
			server.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static Server initServer() {
		Server srv = new Server(8673);
    	ResourceHandler handler = new ResourceHandler();
    	handler.setResourceBase(ProxyObjectFactory.getDocumentsFolder());
        
        Context context = new Context(srv, "/servlets");
        //context.addServlet("com.metamatrix.test.servlet.EchoServlet", "/Echo");
        //context.addServlet("com.metamatrix.test.servlet.MockResponseServlet", "/Mock");
        context.addServlet("com.metamatrix.test.servlet.NameValueServlet", "/requestTest/NameValue");
        //context.addServlet("com.metamatrix.test.servlet.DocNameValueServlet", "/requestTest/docNameValue");
        //context.addServlet("com.metamatrix.test.servlet.DocPostBodyServlet", "/requestTest/docPostBody");
		
        HandlerList hList = new HandlerList();
        hList.setHandlers(new Handler[]{handler, context, new DefaultHandler()});
        srv.setHandler(hList);
        return srv;
	}

	public void stop() {
		try {
			server.stop();
		} catch (Exception e) {
			// shutting down.
		}
	}

}