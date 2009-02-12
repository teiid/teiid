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



package com.metamatrix.connector.xml.jms;

import java.util.Properties;

import javax.jms.DeliveryMode;
import javax.jms.Session;

import com.metamatrix.connector.api.Connection;
import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.xml.CachingConnector;
import com.metamatrix.connector.xml.DocumentProducer;
import com.metamatrix.connector.xml.XMLExecution;
import com.metamatrix.connector.xml.base.SecureConnectorStateImpl;

/**
 * This class is responsible for getting the configuration data from the binding and asserting
 * that it is consistent with the needs of the connector.
 * @author Jdoyle
 *
 */
public class JMSXMLConnectorState extends SecureConnectorStateImpl implements JMSConnectorState {
	
	private String initialContextFactoryName;
	private String primaryProviderUrl;
	private String connectionFactoryName;
	private String outboundJMSDestination;
	private String inboundJMSDestination;
	private String userName;
	private String password;
	private int acknowledgementMode;
	private int receiveTimeout;
	private String correlationIdPrefix;
	private int messsagePriority;
	private int messageDuration;
	private int messageDeliveryMode;
	private String replyToDestination;
	private int connectionRetryCount;

	public JMSXMLConnectorState(){};

	public void setState(ConnectorEnvironment env) throws ConnectorException {
		super.setState(env);
		Properties properties = env.getProperties();
		String provisionalContextFactory = properties.getProperty(INITIAL_CONTEXT_FACTORY);
		if(isNotNullOrEmpty(provisionalContextFactory)) {
			setInitialContextFactoryName(provisionalContextFactory);
		} else {
			throw new ConnectorException(Messages.getString("JMSConnectorState.invalid.INITIAL_CONTEXT_FACTORY"));
		}
		
		String provisionalPrimaryProviderURL = properties.getProperty(PROVIDER_URL);
		if(isNotNullOrEmpty(provisionalPrimaryProviderURL)) {
			setPrimaryProviderUrl(provisionalPrimaryProviderURL);
		} else {
			throw new ConnectorException(Messages.getString("JMSConnectorState.invalid.PROVIDER_URL"));
		}
		
		String provisionalConnectionFactoryName = properties.getProperty(CONNECTION_FACTORY);
		if(isNotNullOrEmpty(provisionalConnectionFactoryName)) {
			setConnectionFactoryName(provisionalConnectionFactoryName);
		} else {
			throw new ConnectorException(Messages.getString("JMSConnectorState.invalid.CONNECTION_FACTORY"));
		}
		
		String provisionalInboundJMSDestination = properties.getProperty(INBOUND_JMS_DESTINATION);
		if(isNotNullOrEmpty(provisionalInboundJMSDestination)) {
			setInboundJMSDestination(provisionalInboundJMSDestination);
		} else {
			logger.logInfo(Messages.getString("JMSConnectorState.empty.INBOUND_JMS_DESTINATION"));
		}
		
		String provisionalOutboundJMSDestination = properties.getProperty(OUTBOUND_JMS_DESTINATION);
		if(isNotNullOrEmpty(provisionalOutboundJMSDestination)) {
			setOutboundJMSDestination(provisionalOutboundJMSDestination);
		} else {
			logger.logInfo(Messages.getString("JMSConnectorState.empty.OUTBOUND_JMS_DESTINATION"));
		}
		
		if(getOutboundJMSDestination() == null && getInboundJMSDestination() == null) {
			throw new ConnectorException(Messages.getString("JMSConnectorState.empty.destinations"));
		}
		
		String provisionalUserName = properties.getProperty(USERNAME);
		if(isNotNullOrEmpty(provisionalUserName)) {
			setUserName(provisionalUserName);
		} else {
			logger.logInfo(Messages.getString("JMSConnectorState.invalid.USERNAME"));
		}
		
		String provisionalPassword = properties.getProperty(PASSWORD);
		if(isNotNullOrEmpty(provisionalPassword)) {
			setPassword(provisionalPassword);
		} else {
			logger.logInfo(Messages.getString("JMSConnectorState.invalid.PASSWORD"));
		}
		
		String provisionalAcknowledgementMode = properties.getProperty(ACKNOWLEDGEMENT_MODE);
		if(isNotNullOrEmpty(provisionalAcknowledgementMode)) {
			int mode = encodeAcknowledgementMode(provisionalAcknowledgementMode);
			setAcknowledgementMode(mode);
		} else {
			throw new ConnectorException(Messages.getString("JMSConnectorState.empty.ack.mode"));
		}
		
		//TODO: Decide if we want to enforce a limit on values here, it's possible to hang here forever.
		try {
			int provisionalReceiveTimeout = Integer.parseInt(properties.getProperty(RECEIVE_TIMEOUT));
			setReceiveTimeout(provisionalReceiveTimeout);
		} catch (NumberFormatException e) {
			throw new ConnectorException(Messages.getString("JMSConnectorState.invalid.receive.timeout"));
		}
		
		String provisionalCorrelationIdPrefix = properties.getProperty(CORRELATION_ID);
		if(isNotNullOrEmpty(provisionalCorrelationIdPrefix)) {
			setCorrelationIdPrefix(provisionalCorrelationIdPrefix);
		} else {
			logger.logInfo(Messages.getString("JMSConnectorState.empty.correlation.id"));
		}
		
		try {
			int provisionalMessagePriority = Integer.parseInt(properties.getProperty(MESSAGE_PRIORITY));
			if(provisionalMessagePriority < 1 || provisionalMessagePriority > 9) {
				throw new ConnectorException(Messages.getString("JMSConnectorState.invalid.message.priority"));
			}
			setMesssagePriority(provisionalMessagePriority);
		} catch (NumberFormatException e) {
			throw new ConnectorException(Messages.getString("JMSConnectorState.message.priority.format"));
		}
		
		try {
			int provisionalMessageDuration = Integer.parseInt(properties.getProperty(MESSAGE_DURATION));
			setMessageDuration(provisionalMessageDuration);
		} catch (NumberFormatException e) {
			throw new ConnectorException(Messages.getString("JMSConnectorState.message.duration.format"));
		}
		
		String provisionalDeliveryMode = properties.getProperty(MESSAGE_DELIVERY_MODE);
		if(isNotNullOrEmpty(provisionalDeliveryMode)) {
			int mode = encodeMessageDeliveryMode(provisionalDeliveryMode);
			setMessageDeliveryMode(mode);
		} else {
			throw new ConnectorException(Messages.getString("JMSConnectorState.empty.delivery.mode"));
		}
		
		String provisionalReplyToDestination = properties.getProperty(REPLY_TO_DESTINATION);
		if(isNotNullOrEmpty(provisionalReplyToDestination)) {
			setReplyToDestination(provisionalReplyToDestination);
		} else {
			logger.logInfo(Messages.getString("JMSConnectorState.empty.reply.to"));
		}
		
		try {
			int provisionalConnectionRetryCount = Integer.parseInt(properties.getProperty(CONNECTION_RETRY_COUNT));
			setConnectionRetryCount(provisionalConnectionRetryCount);
		} catch (NumberFormatException e) {
			throw new ConnectorException(Messages.getString("JMSConnectorState.connection.retry.format"));
		}
		
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.base.XMLConnectorStateImpl#makeExecutor(com.metamatrix.connector.xml.base.XMLExecutionImpl)
	 */
	public DocumentProducer makeExecutor(XMLExecution execution) throws ConnectorException {
		return new JMSRequestExecutor(this, (JMSExecution) execution);
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.jms.JMSConnectorState#getInitialContextFactoryName()
	 */
	public String getInitialContextFactoryName() {
		return initialContextFactoryName;
	}

	private void setInitialContextFactoryName(String initialContextFactoryName) {
		this.initialContextFactoryName = initialContextFactoryName;
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.jms.JMSConnectorState#getPrimaryProviderUrl()
	 */
	public String getPrimaryProviderUrl() {
		return primaryProviderUrl;
	}

	private void setPrimaryProviderUrl(String primaryProviderUrl) {
		this.primaryProviderUrl = primaryProviderUrl;
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.jms.JMSConnectorState#getConnectionFactoryName()
	 */
	public String getConnectionFactoryName() {
		return connectionFactoryName;
	}

	private void setConnectionFactoryName(String connectionFactoryName) {
		this.connectionFactoryName = connectionFactoryName;
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.jms.JMSConnectorState#getUserName()
	 */
	public String getUserName() {
		return userName;
	}

	private void setUserName(String userName) {
		this.userName = userName;
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.jms.JMSConnectorState#getPassword()
	 */
	public String getPassword() {
		return password;
	}

	private void setPassword(String password) {
		this.password = password;
	}
	
	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.jms.JMSConnectorState#getInboundJMSDestination()
	 */
	public String getInboundJMSDestination() {
		return inboundJMSDestination;
	}

	private void setInboundJMSDestination(String inboundJMSDestination) {
		this.inboundJMSDestination = inboundJMSDestination;
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.jms.JMSConnectorState#getOutboundJMSDestination()
	 */
	public String getOutboundJMSDestination() {
		return outboundJMSDestination;
	}

	private void setOutboundJMSDestination(String outboundJMSDestination) {
		this.outboundJMSDestination = outboundJMSDestination;
	}

	private int encodeAcknowledgementMode(String mode) throws ConnectorException {
		int result;
		if(mode.toLowerCase().equals(CLIENT_ACKNOWLEDGE.toLowerCase())){
			result = Session.CLIENT_ACKNOWLEDGE;
		} else if(mode.toLowerCase().equals(AUTO_ACKNOWLEDGE.toLowerCase())){
			result = Session.CLIENT_ACKNOWLEDGE;
		} else if(mode.toLowerCase().equals(DUPS_OK_ACKNOWLEDGE.toLowerCase())){
			result = Session.DUPS_OK_ACKNOWLEDGE;
		} else {
			throw new ConnectorException(Messages.getString("JMSConnectorState.invalid.ack.mode"));
		}
		return result;
	}

	private int encodeMessageDeliveryMode(String mode) throws ConnectorException {
		int result;
		if(mode.toLowerCase().equals(DELIVERY_MODE_NON_PERISTENT.toLowerCase())){
			result = DeliveryMode.NON_PERSISTENT;
		} else if(mode.toLowerCase().equals(DELIVERY_MODE_PERISTENT.toLowerCase())){
			result = DeliveryMode.PERSISTENT;
		} else {
			throw new ConnectorException(Messages.getString("JMSConnectorState.invalid.delivery.mode"));
		}
		return result;
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.jms.JMSConnectorState#getAcknowledgementMode()
	 */
	public int getAcknowledgementMode() {
		return acknowledgementMode;
	}

	private void setAcknowledgementMode(int acknowledgementMode) {
		this.acknowledgementMode = acknowledgementMode;
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.jms.JMSConnectorState#getReceiveTimeout()
	 */
	public int getReceiveTimeout() {
		return receiveTimeout;
	}

	private void setReceiveTimeout(int receiveTimeout) {
		this.receiveTimeout = receiveTimeout;
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.jms.JMSConnectorState#getCorrelationIdPrefix()
	 */
	public String getCorrelationIdPrefix() {
		return correlationIdPrefix;
	}

	private void setCorrelationIdPrefix(String correlationIdPrefix) {
		this.correlationIdPrefix = correlationIdPrefix;
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.jms.JMSConnectorState#getMesssagePriority()
	 */
	public int getMesssagePriority() {
		return messsagePriority;
	}

	private void setMesssagePriority(int messsagePriority) {
		this.messsagePriority = messsagePriority;
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.jms.JMSConnectorState#getMessageDuration()
	 */
	public int getMessageDuration() {
		return messageDuration;
	}

	private void setMessageDuration(int messageDuration) {
		this.messageDuration = messageDuration;
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.jms.JMSConnectorState#getMessageDeliveryMode()
	 */
	public int getMessageDeliveryMode() {
		return messageDeliveryMode;
	}

	private void setMessageDeliveryMode(int messageDeliveryMode) {
		this.messageDeliveryMode = messageDeliveryMode;
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.jms.JMSConnectorState#getReplyToDestination()
	 */
	public String getReplyToDestination() {
		return replyToDestination;
	}

	private void setReplyToDestination(String replyToDestination) {
		this.replyToDestination = replyToDestination;
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.jms.JMSConnectorState#getConnectionRetryCount()
	 */
	public int getConnectionRetryCount() {
		return connectionRetryCount;
	}

	private void setConnectionRetryCount(int connectionRetryCount) {
		this.connectionRetryCount = connectionRetryCount;
	}

	public Connection getConnection(CachingConnector connector, ExecutionContext context, ConnectorEnvironment environment) throws ConnectorException {
		return new JMSConnection(connector, context, environment);
	}

	public Properties getState() {
		Properties props = super.getState();
		
		return props;
	}
}
