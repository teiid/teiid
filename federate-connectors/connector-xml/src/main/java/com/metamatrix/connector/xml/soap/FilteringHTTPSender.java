package com.metamatrix.connector.xml.soap;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;

import org.apache.commons.httpclient.HttpMethodBase;

import com.metamatrix.connector.xml.XMLConnectorState;
import com.metamatrix.connector.xml.base.LoggingInputStreamFilter;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.exception.ConnectorException;

public class FilteringHTTPSender extends CommonsHTTPSender {

	private XMLConnectorState state;
	private ConnectorLogger logger;

	public FilteringHTTPSender(XMLConnectorState state, ConnectorLogger logger) {
		this.state = state;
		this.logger = logger;
	}

	@Override
	public InputStream createStreamFilters(HttpMethodBase method)
			throws Exception {
		InputStream result = super.createStreamFilters(method);
		if(state.isLogRequestResponse()) {
			result = new LoggingInputStreamFilter(result, logger);
		}
		
		Class pluggableFilter = Class.forName(state.getPluggableInputStreamFilterClass());
		Constructor ctor = pluggableFilter.getConstructor(
			new Class[] { java.io.InputStream.class, com.metamatrix.data.api.ConnectorLogger.class});
		result = (InputStream) ctor.newInstance(new Object[] {result, logger});
		return result;
	}
}
