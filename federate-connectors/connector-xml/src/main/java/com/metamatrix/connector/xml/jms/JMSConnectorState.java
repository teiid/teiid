/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import com.metamatrix.connector.xml.SecureConnectorState;

public interface JMSConnectorState extends SecureConnectorState {

	public static final String INITIAL_CONTEXT_FACTORY = "INITIAL_CONTEXT_FACTORY";

	public static final String PROVIDER_URL = "PROVIDER_URL";

	public static final String CONNECTION_FACTORY = "CONNECTION_FACTORY";

	public static final String INBOUND_JMS_DESTINATION = "INBOUND_JMS_DESTINATION";

	public static final String OUTBOUND_JMS_DESTINATION = "OUTBOUND_JMS_DESTINATION";

	public static final String ACKNOWLEDGEMENT_MODE = "ACKNOWLEDGEMENT_MODE";

	public static final String AUTO_ACKNOWLEDGE = "AUTO_ACKNOWLEDGE";

	public static final String CLIENT_ACKNOWLEDGE = "CLIENT_ACKNOWLEDGE";

	public static final String DUPS_OK_ACKNOWLEDGE = "DUPS_OK_ACKNOWLEDGE";

	public static final String RECEIVE_TIMEOUT = "RECEIVE_TIMEOUT";

	public static final String CORRELATION_ID = "CORRELATION_ID";

	public static final String MESSAGE_PRIORITY = "MESSAGE_PRIORITY";

	public static final String MESSAGE_DURATION = "MESSAGE_DURATION";

	public static final String MESSAGE_DELIVERY_MODE = "MESSAGE_DELIVERY_MODE";

	public static final String DELIVERY_MODE_PERISTENT = "DELIVERY_MODE_PERISTENT";

	public static final String DELIVERY_MODE_NON_PERISTENT = "DELIVERY_MODE_NON_PERISTENT";

	public static final String REPLY_TO_DESTINATION = "REPLY_TO_DESTINATION";

	public static final String USERNAME = "USERNAME";

	public static final String PASSWORD = "PASSWORD";

	public static final String CONNECTION_RETRY_COUNT = "CONNECTION_RETRY_COUNT";

	public abstract String getInitialContextFactoryName();

	public abstract String getPrimaryProviderUrl();

	public abstract String getConnectionFactoryName();

	public abstract String getUserName();

	public abstract String getPassword();

	public abstract String getInboundJMSDestination();

	public abstract String getOutboundJMSDestination();

	public abstract int getAcknowledgementMode();

	public abstract int getReceiveTimeout();

	public abstract String getCorrelationIdPrefix();

	public abstract int getMesssagePriority();

	public abstract int getMessageDuration();

	public abstract int getMessageDeliveryMode();

	public abstract String getReplyToDestination();

	public abstract int getConnectionRetryCount();

}