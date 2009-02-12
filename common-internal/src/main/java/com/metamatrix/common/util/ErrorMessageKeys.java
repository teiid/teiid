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

package com.metamatrix.common.util;

/**
 * Date Apr 2, 2003
 *
 * <p>
 * The ErrorMessageKeys contains the message ID's for use with
 * {@link I18NLogManager I18NLogManager} for internationalization
 * of error messages.
 * </p>
 *
 * <b>Adding a Message ID</b>
 * <br>
 * An error message placed here <b>MUST</b> have a related entry
 * in the project resource bundle file.
 * </br>
 * <br>
 * The format of the message ID should conform to the following convention:
 * </br>
 * ERR.000.000.0000
 *
 * <strong>Example:</strong>
 * <code>ERR.003.001.0002</code>
 *
 * where
 * - node 003 is the common project number
 * - node 001 is the component and must be unique for the project
 * - node 0002 is a unique number for the specified component
 *
 *
 *
 * <p>
 * <strong>Common Component Codes</strong>
 * <li>000 - misc</li>
 * <li>001 - config</li>
 * <li>002 - pooling</li>
 * <li>003 - api</li>
 * <li>004 - actions</li>
 * <li>005 - beans</li>
 * <li>006 - buffering</li>
 * <li>007 - util</li>
 * <li>008 - cache</li>
 * <li>009 - callback</li>
 * <li>010 - connecteion</li>
 * <li>011 - event</li>
 * <li>012 - finder</li>
 * <li>013 - id</li>
 * <li>014 - log</li>
 * <li>015 - jdbc</li>
 * <li>016 - license</li>
 * <li>017 - messaging</li>
 * <li>018 - namedobject</li>
 * <li>019 - object</li>
 * <li>020 - plugin</li>
 * <li>021 - properties</li>
 * <li>022 - proxy</li>
 * <li>023 - queue</li>
 * <li>024 - remote</li>
 * <li>025 - thread</li>
 * <li>026 - transaction</li>
 * <li>027 - transform</li>
 * <li>028 - tree</li>
 * <li>029 - types</li>
 * <li>030 - util</li>
 * <li>031 - xa</li>
 * <li>032 - xml</li>
 *
 *
 * </p>
 */
public interface ErrorMessageKeys {

	/** misc (000) */
		public static final String MISC_ERR_0001 = "ERR.003.000.0001"; //$NON-NLS-1$


	/** config (001) */
        // moved from platform
        public static final String CONFIG_0001 = "ERR.003.001.0093"; //$NON-NLS-1$
        public static final String CONFIG_0002 = "ERR.003.001.0094"; //$NON-NLS-1$
        public static final String CONFIG_0003 = "ERR.003.001.0095"; //$NON-NLS-1$
        public static final String CONFIG_0004 = "ERR.003.001.0096"; //$NON-NLS-1$
        public static final String CONFIG_0005 = "ERR.003.001.0097"; //$NON-NLS-1$
        public static final String CONFIG_0006 = "ERR.003.001.0098"; //$NON-NLS-1$
        public static final String CONFIG_0016 = "ERR.003.001.0099"; //$NON-NLS-1$
        public static final String CONFIG_0017 = "ERR.003.001.0100"; //$NON-NLS-1$
        public static final String CONFIG_0018 = "ERR.003.001.0101"; //$NON-NLS-1$



		public static final String CONFIG_ERR_0001 = "ERR.003.001.0001"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0002 = "ERR.003.001.0002"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0003 = "ERR.003.001.0003"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0004 = "ERR.003.001.0004"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0005 = "ERR.003.001.0005"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0006 = "ERR.003.001.0006"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0007 = "ERR.003.001.0007"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0008 = "ERR.003.001.0008"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0009 = "ERR.003.001.0009"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0010 = "ERR.003.001.0010"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0011 = "ERR.003.001.0011"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0012 = "ERR.003.001.0012"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0013 = "ERR.003.001.0013"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0014 = "ERR.003.001.0014"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0015 = "ERR.003.001.0015"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0016 = "ERR.003.001.0016"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0017 = "ERR.003.001.0017"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0018 = "ERR.003.001.0018"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0019 = "ERR.003.001.0019"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0020 = "ERR.003.001.0020"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0021 = "ERR.003.001.0021"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0022 = "ERR.003.001.0022"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0023 = "ERR.003.001.0023"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0024 = "ERR.003.001.0024"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0025 = "ERR.003.001.0025"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0026 = "ERR.003.001.0026"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0027 = "ERR.003.001.0027"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0028 = "ERR.003.001.0028"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0029 = "ERR.003.001.0029"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0030 = "ERR.003.001.0030"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0031 = "ERR.003.001.0031"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0032 = "ERR.003.001.0032"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0033 = "ERR.003.001.0033"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0034 = "ERR.003.001.0034"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0035 = "ERR.003.001.0035"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0036 = "ERR.003.001.0036"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0037 = "ERR.003.001.0037"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0038 = "ERR.003.001.0038"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0039 = "ERR.003.001.0039"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0040 = "ERR.003.001.0040"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0041 = "ERR.003.001.0041"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0042 = "ERR.003.001.0042"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0043 = "ERR.003.001.0043"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0044 = "ERR.003.001.0044"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0045 = "ERR.003.001.0045"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0046 = "ERR.003.001.0046"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0047 = "ERR.003.001.0047"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0048 = "ERR.003.001.0048"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0049 = "ERR.003.001.0049"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0050 = "ERR.003.001.0050"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0051 = "ERR.003.001.0051"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0052 = "ERR.003.001.0052"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0053 = "ERR.003.001.0053"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0054 = "ERR.003.001.0054"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0055 = "ERR.003.001.0055"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0056 = "ERR.003.001.0056"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0057 = "ERR.003.001.0057"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0058 = "ERR.003.001.0058"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0059 = "ERR.003.001.0059"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0060 = "ERR.003.001.0060"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0061 = "ERR.003.001.0061"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0062 = "ERR.003.001.0062"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0063 = "ERR.003.001.0063"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0064 = "ERR.003.001.0064"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0065 = "ERR.003.001.0065"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0066 = "ERR.003.001.0066"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0067 = "ERR.003.001.0067"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0068 = "ERR.003.001.0068"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0069 = "ERR.003.001.0069"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0070 = "ERR.003.001.0070"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0071 = "ERR.003.001.0071"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0072 = "ERR.003.001.0072"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0073 = "ERR.003.001.0073"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0074 = "ERR.003.001.0074"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0075 = "ERR.003.001.0075"; //$NON-NLS-1$
		public static final String CONFIG_ERR_0076 = "ERR.003.001.0076"; //$NON-NLS-1$

        public static final String CONFIG_ERR_0077 = "ERR.003.001.0077"; //$NON-NLS-1$
        public static final String CONFIG_ERR_0078 = "ERR.003.001.0078"; //$NON-NLS-1$
        public static final String CONFIG_ERR_0079 = "ERR.003.001.0079"; //$NON-NLS-1$
        public static final String CONFIG_ERR_0080 = "ERR.003.001.0080"; //$NON-NLS-1$
        public static final String CONFIG_ERR_0081 = "ERR.003.001.0081"; //$NON-NLS-1$
        public static final String CONFIG_ERR_0082 = "ERR.003.001.0082"; //$NON-NLS-1$
        public static final String CONFIG_ERR_0083 = "ERR.003.001.0083"; //$NON-NLS-1$
        public static final String CONFIG_ERR_0084 = "ERR.003.001.0084"; //$NON-NLS-1$
        public static final String CONFIG_ERR_0085 = "ERR.003.001.0085"; //$NON-NLS-1$
        public static final String CONFIG_ERR_0086 = "ERR.003.001.0086"; //$NON-NLS-1$
        public static final String CONFIG_ERR_0087 = "ERR.003.001.0087"; //$NON-NLS-1$
        public static final String CONFIG_ERR_0088 = "ERR.003.001.0088"; //$NON-NLS-1$
        public static final String CONFIG_ERR_0089 = "ERR.003.001.0089"; //$NON-NLS-1$
        public static final String CONFIG_ERR_0090 = "ERR.003.001.0090"; //$NON-NLS-1$
        public static final String CONFIG_ERR_0091 = "ERR.003.001.0091"; //$NON-NLS-1$
        public static final String CONFIG_ERR_0092 = "ERR.003.001.0092"; //$NON-NLS-1$



	/** pooling (002 */

		public static final String POOLING_ERR_0001 = "ERR.003.002.0001"; //$NON-NLS-1$
		public static final String POOLING_ERR_0002 = "ERR.003.002.0002"; //$NON-NLS-1$
		public static final String POOLING_ERR_0003 = "ERR.003.002.0003"; //$NON-NLS-1$
		public static final String POOLING_ERR_0005 = "ERR.003.002.0005"; //$NON-NLS-1$
		public static final String POOLING_ERR_0006 = "ERR.003.002.0006"; //$NON-NLS-1$
		public static final String POOLING_ERR_0007 = "ERR.003.002.0007"; //$NON-NLS-1$
		public static final String POOLING_ERR_0008 = "ERR.003.002.0008"; //$NON-NLS-1$
		public static final String POOLING_ERR_0009 = "ERR.003.002.0009"; //$NON-NLS-1$
		public static final String POOLING_ERR_0010 = "ERR.003.002.0010"; //$NON-NLS-1$
		public static final String POOLING_ERR_0011 = "ERR.003.002.0011"; //$NON-NLS-1$
		public static final String POOLING_ERR_0012 = "ERR.003.002.0012"; //$NON-NLS-1$
		public static final String POOLING_ERR_0013 = "ERR.003.002.0013"; //$NON-NLS-1$
		public static final String POOLING_ERR_0014 = "ERR.003.002.0014"; //$NON-NLS-1$
		public static final String POOLING_ERR_0015 = "ERR.003.002.0015"; //$NON-NLS-1$
		public static final String POOLING_ERR_0016 = "ERR.003.002.0016"; //$NON-NLS-1$
		public static final String POOLING_ERR_0017 = "ERR.003.002.0017"; //$NON-NLS-1$
		public static final String POOLING_ERR_0018 = "ERR.003.002.0018"; //$NON-NLS-1$
		public static final String POOLING_ERR_0019 = "ERR.003.002.0019"; //$NON-NLS-1$

// #s 20 - 35 were moved to util

		public static final String POOLING_ERR_0026 = "ERR.003.002.0026"; //$NON-NLS-1$
		public static final String POOLING_ERR_0027 = "ERR.003.002.0027"; //$NON-NLS-1$
		public static final String POOLING_ERR_0028 = "ERR.003.002.0028"; //$NON-NLS-1$
		public static final String POOLING_ERR_0029 = "ERR.003.002.0029"; //$NON-NLS-1$
		public static final String POOLING_ERR_0030 = "ERR.003.002.0030"; //$NON-NLS-1$
		public static final String POOLING_ERR_0031 = "ERR.003.002.0031"; //$NON-NLS-1$
		public static final String POOLING_ERR_0032 = "ERR.003.002.0032"; //$NON-NLS-1$
		public static final String POOLING_ERR_0033 = "ERR.003.002.0033"; //$NON-NLS-1$
		public static final String POOLING_ERR_0034 = "ERR.003.002.0034"; //$NON-NLS-1$
		public static final String POOLING_ERR_0035 = "ERR.003.002.0035"; //$NON-NLS-1$
		public static final String POOLING_ERR_0036 = "ERR.003.002.0036"; //$NON-NLS-1$
		public static final String POOLING_ERR_0037 = "ERR.003.002.0037"; //$NON-NLS-1$

		public static final String POOLING_ERR_0038 = "ERR.003.002.0038"; //$NON-NLS-1$
		public static final String POOLING_ERR_0039 = "ERR.003.002.0039"; //$NON-NLS-1$

//*** not used because they were not needed at the time, but can be used now
//		public static final String POOLING_ERR_0040 = "ERR.003.002.0040";
//		public static final String POOLING_ERR_0041 = "ERR.003.002.0041";
//		public static final String POOLING_ERR_0042 = "ERR.003.002.0042";
//		public static final String POOLING_ERR_0043 = "ERR.003.002.0043";

		public static final String POOLING_ERR_0044 = "ERR.003.002.0044"; //$NON-NLS-1$
		public static final String POOLING_ERR_0045 = "ERR.003.002.0045"; //$NON-NLS-1$
		public static final String POOLING_ERR_0046 = "ERR.003.002.0046"; //$NON-NLS-1$
		public static final String POOLING_ERR_0047 = "ERR.003.002.0047"; //$NON-NLS-1$
		public static final String POOLING_ERR_0048 = "ERR.003.002.0048"; //$NON-NLS-1$
		public static final String POOLING_ERR_0049 = "ERR.003.002.0049"; //$NON-NLS-1$

/** api (003) */

		public static final String API_ERR_0001 = "ERR.003.003.0001"; //$NON-NLS-1$
		public static final String API_ERR_0002 = "ERR.003.003.0002"; //$NON-NLS-1$
		public static final String API_ERR_0003 = "ERR.003.003.0003"; //$NON-NLS-1$
		public static final String API_ERR_0004 = "ERR.003.003.0004"; //$NON-NLS-1$
		public static final String API_ERR_0005 = "ERR.003.003.0005"; //$NON-NLS-1$
		public static final String API_ERR_0006 = "ERR.003.003.0006"; //$NON-NLS-1$
		public static final String API_ERR_0007 = "ERR.003.003.0007"; //$NON-NLS-1$
		public static final String API_ERR_0008 = "ERR.003.003.0008"; //$NON-NLS-1$
		public static final String API_ERR_0009 = "ERR.003.003.0009"; //$NON-NLS-1$
		public static final String API_ERR_0010 = "ERR.003.003.0010"; //$NON-NLS-1$
		public static final String API_ERR_0011 = "ERR.003.003.0011"; //$NON-NLS-1$
		public static final String API_ERR_0012 = "ERR.003.003.0012"; //$NON-NLS-1$
		public static final String API_ERR_0013 = "ERR.003.003.0013"; //$NON-NLS-1$
		public static final String API_ERR_0014 = "ERR.003.003.0014"; //$NON-NLS-1$
		public static final String API_ERR_0015 = "ERR.003.003.0015"; //$NON-NLS-1$
		public static final String API_ERR_0016 = "ERR.003.003.0016"; //$NON-NLS-1$
		public static final String API_ERR_0017 = "ERR.003.003.0017"; //$NON-NLS-1$
		public static final String API_ERR_0018 = "ERR.003.003.0018"; //$NON-NLS-1$
		public static final String API_ERR_0019 = "ERR.003.003.0019"; //$NON-NLS-1$
		public static final String API_ERR_0020 = "ERR.003.003.0020"; //$NON-NLS-1$
		public static final String API_ERR_0021 = "ERR.003.003.0021"; //$NON-NLS-1$
		public static final String API_ERR_0022 = "ERR.003.003.0022"; //$NON-NLS-1$


/** actions (004) */

		public static final String ACTIONS_ERR_0001 = "ERR.003.004.0001"; //$NON-NLS-1$
		public static final String ACTIONS_ERR_0002 = "ERR.003.004.0002"; //$NON-NLS-1$
		public static final String ACTIONS_ERR_0003 = "ERR.003.004.0003"; //$NON-NLS-1$
		public static final String ACTIONS_ERR_0004 = "ERR.003.004.0004"; //$NON-NLS-1$
		public static final String ACTIONS_ERR_0005 = "ERR.003.004.0005"; //$NON-NLS-1$
		public static final String ACTIONS_ERR_0006 = "ERR.003.004.0006"; //$NON-NLS-1$
		public static final String ACTIONS_ERR_0007 = "ERR.003.004.0007"; //$NON-NLS-1$
		public static final String ACTIONS_ERR_0008 = "ERR.003.004.0008"; //$NON-NLS-1$
		public static final String ACTIONS_ERR_0009 = "ERR.003.004.0009"; //$NON-NLS-1$
		public static final String ACTIONS_ERR_0010 = "ERR.003.004.0010"; //$NON-NLS-1$
		public static final String ACTIONS_ERR_0011 = "ERR.003.004.0011"; //$NON-NLS-1$
		public static final String ACTIONS_ERR_0012 = "ERR.003.004.0012"; //$NON-NLS-1$
		public static final String ACTIONS_ERR_0013 = "ERR.003.004.0013"; //$NON-NLS-1$
		public static final String ACTIONS_ERR_0014 = "ERR.003.004.0014"; //$NON-NLS-1$

/** beans (005) */

		public static final String BEANS_ERR_0001 = "ERR.003.005.0001"; //$NON-NLS-1$
		public static final String BEANS_ERR_0002 = "ERR.003.005.0002"; //$NON-NLS-1$
		public static final String BEANS_ERR_0003 = "ERR.003.005.0003"; //$NON-NLS-1$

/** buffering (006) */
		public static final String BUFFERING_ERR_0003 = "ERR.003.006.0003"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0004 = "ERR.003.006.0004"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0005 = "ERR.003.006.0005"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0006 = "ERR.003.006.0006"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0007 = "ERR.003.006.0007"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0009 = "ERR.003.006.0009"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0010 = "ERR.003.006.0010"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0011 = "ERR.003.006.0011"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0012 = "ERR.003.006.0012"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0013 = "ERR.003.006.0013"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0014 = "ERR.003.006.0014"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0015 = "ERR.003.006.0015"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0016 = "ERR.003.006.0016"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0017 = "ERR.003.006.0017"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0018 = "ERR.003.006.0018"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0019 = "ERR.003.006.0019"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0020 = "ERR.003.006.0020"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0021 = "ERR.003.006.0021"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0022 = "ERR.003.006.0022"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0023 = "ERR.003.006.0023"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0024 = "ERR.003.006.0024"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0025 = "ERR.003.006.0025"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0026 = "ERR.003.006.0026"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0027 = "ERR.003.006.0027"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0028 = "ERR.003.006.0028"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0029 = "ERR.003.006.0029"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0030 = "ERR.003.006.0030"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0031 = "ERR.003.006.0031"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0032 = "ERR.003.006.0032"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0033 = "ERR.003.006.0033"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0034 = "ERR.003.006.0034"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0035 = "ERR.003.006.0035"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0036 = "ERR.003.006.0036"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0037 = "ERR.003.006.0037"; //$NON-NLS-1$
		public static final String BUFFERING_ERR_0038 = "ERR.003.006.0038"; //$NON-NLS-1$		
        public static final String BUFFERING_ERR_0039 = "ERR.003.006.0039"; //$NON-NLS-1$       

	/** util (007) */
		// this should be in the same package with util (030)
		public static final String UTIL_ERR_0001 = "ERR.003.007.0001"; //$NON-NLS-1$
		public static final String UTIL_ERR_0002 = "ERR.003.007.0002"; //$NON-NLS-1$
		public static final String UTIL_ERR_0003 = "ERR.003.007.0003"; //$NON-NLS-1$
		public static final String UTIL_ERR_0004 = "ERR.003.007.0004"; //$NON-NLS-1$
		public static final String UTIL_ERR_0005 = "ERR.003.007.0005"; //$NON-NLS-1$
		public static final String UTIL_ERR_0006 = "ERR.003.007.0006"; //$NON-NLS-1$


	/** cache (008) */
		public static final String CACHE_ERR_0001 = "ERR.003.008.0001"; //$NON-NLS-1$
		public static final String CACHE_ERR_0002 = "ERR.003.008.0002"; //$NON-NLS-1$
		public static final String CACHE_ERR_0003 = "ERR.003.008.0003"; //$NON-NLS-1$
		public static final String CACHE_ERR_0004 = "ERR.003.008.0004"; //$NON-NLS-1$
		public static final String CACHE_ERR_0005 = "ERR.003.008.0005"; //$NON-NLS-1$
		public static final String CACHE_ERR_0006 = "ERR.003.008.0006"; //$NON-NLS-1$
		public static final String CACHE_ERR_0007 = "ERR.003.008.0007"; //$NON-NLS-1$
		public static final String CACHE_ERR_0008 = "ERR.003.008.0008"; //$NON-NLS-1$
		public static final String CACHE_ERR_0009 = "ERR.003.008.0009"; //$NON-NLS-1$
		public static final String CACHE_ERR_0010 = "ERR.003.008.0010"; //$NON-NLS-1$
		public static final String CACHE_ERR_0011 = "ERR.003.008.0011"; //$NON-NLS-1$
		public static final String CACHE_ERR_0012 = "ERR.003.008.0012"; //$NON-NLS-1$
		public static final String CACHE_ERR_0013 = "ERR.003.008.0013"; //$NON-NLS-1$
		public static final String CACHE_ERR_0014 = "ERR.003.008.0014"; //$NON-NLS-1$
		public static final String CACHE_ERR_0015 = "ERR.003.008.0015"; //$NON-NLS-1$
		public static final String CACHE_ERR_0016 = "ERR.003.008.0016"; //$NON-NLS-1$
		public static final String CACHE_ERR_0017 = "ERR.003.008.0017"; //$NON-NLS-1$
		public static final String CACHE_ERR_0018 = "ERR.003.008.0018"; //$NON-NLS-1$
		public static final String CACHE_ERR_0019 = "ERR.003.008.0019"; //$NON-NLS-1$
		public static final String CACHE_ERR_0020 = "ERR.003.008.0020"; //$NON-NLS-1$
		public static final String CACHE_ERR_0021 = "ERR.003.008.0021"; //$NON-NLS-1$

	/** callback (009) */
		// moved to console.toolbox project
        
	/** connection (010) */
		public static final String CONNECTION_ERR_0001 = "ERR.003.010.0001"; //$NON-NLS-1$
		public static final String CONNECTION_ERR_0002 = "ERR.003.010.0002"; //$NON-NLS-1$
		public static final String CONNECTION_ERR_0003 = "ERR.003.010.0003"; //$NON-NLS-1$
		public static final String CONNECTION_ERR_0004 = "ERR.003.010.0004"; //$NON-NLS-1$
		public static final String CONNECTION_ERR_0005 = "ERR.003.010.0005"; //$NON-NLS-1$
		public static final String CONNECTION_ERR_0006 = "ERR.003.010.0006"; //$NON-NLS-1$
		public static final String CONNECTION_ERR_0007 = "ERR.003.010.0007"; //$NON-NLS-1$
		public static final String CONNECTION_ERR_0008 = "ERR.003.010.0008"; //$NON-NLS-1$
		public static final String CONNECTION_ERR_0009 = "ERR.003.010.0009"; //$NON-NLS-1$
		public static final String CONNECTION_ERR_0010 = "ERR.003.010.0010"; //$NON-NLS-1$
		public static final String CONNECTION_ERR_0011 = "ERR.003.010.0011"; //$NON-NLS-1$
		public static final String CONNECTION_ERR_0012 = "ERR.003.010.0012"; //$NON-NLS-1$
		public static final String CONNECTION_ERR_0013 = "ERR.003.010.0013"; //$NON-NLS-1$
		public static final String CONNECTION_ERR_0014 = "ERR.003.010.0014"; //$NON-NLS-1$
		


	/** id (013) */
		public static final String ID_ERR_0001 = "ERR.003.013.0001"; //$NON-NLS-1$
		public static final String ID_ERR_0002 = "ERR.003.013.0002"; //$NON-NLS-1$
		public static final String ID_ERR_0003 = "ERR.003.013.0003"; //$NON-NLS-1$
		public static final String ID_ERR_0004 = "ERR.003.013.0004"; //$NON-NLS-1$
		public static final String ID_ERR_0005 = "ERR.003.013.0005"; //$NON-NLS-1$
		public static final String ID_ERR_0006 = "ERR.003.013.0006"; //$NON-NLS-1$
		public static final String ID_ERR_0007 = "ERR.003.013.0007"; //$NON-NLS-1$
		public static final String ID_ERR_0008 = "ERR.003.013.0008"; //$NON-NLS-1$
		public static final String ID_ERR_0009 = "ERR.003.013.0009"; //$NON-NLS-1$
		public static final String ID_ERR_0010 = "ERR.003.013.0010"; //$NON-NLS-1$
		public static final String ID_ERR_0011 = "ERR.003.013.0011"; //$NON-NLS-1$
		public static final String ID_ERR_0012 = "ERR.003.013.0012"; //$NON-NLS-1$
		public static final String ID_ERR_0013 = "ERR.003.013.0013"; //$NON-NLS-1$
		public static final String ID_ERR_0014 = "ERR.003.013.0014"; //$NON-NLS-1$
		public static final String ID_ERR_0015 = "ERR.003.013.0015"; //$NON-NLS-1$
		public static final String ID_ERR_0016 = "ERR.003.013.0016"; //$NON-NLS-1$
		public static final String ID_ERR_0017 = "ERR.003.013.0017"; //$NON-NLS-1$
		public static final String ID_ERR_0018 = "ERR.003.013.0018"; //$NON-NLS-1$
		public static final String ID_ERR_0019 = "ERR.003.013.0019"; //$NON-NLS-1$
		public static final String ID_ERR_0020 = "ERR.003.013.0020"; //$NON-NLS-1$
		public static final String ID_ERR_0021 = "ERR.003.013.0021"; //$NON-NLS-1$
		public static final String ID_ERR_0022 = "ERR.003.013.0022"; //$NON-NLS-1$
		public static final String ID_ERR_0023 = "ERR.003.013.0023"; //$NON-NLS-1$
		public static final String ID_ERR_0024 = "ERR.003.013.0024"; //$NON-NLS-1$



		/** log (014) */
		public static final String LOG_ERR_0001 = "ERR.003.014.0001"; //$NON-NLS-1$
		public static final String LOG_ERR_0002 = "ERR.003.014.0002"; //$NON-NLS-1$
		public static final String LOG_ERR_0003 = "ERR.003.014.0003"; //$NON-NLS-1$
		public static final String LOG_ERR_0004 = "ERR.003.014.0004"; //$NON-NLS-1$
		public static final String LOG_ERR_0005 = "ERR.003.014.0005"; //$NON-NLS-1$
		public static final String LOG_ERR_0006 = "ERR.003.014.0006"; //$NON-NLS-1$
		public static final String LOG_ERR_0007 = "ERR.003.014.0007"; //$NON-NLS-1$
		public static final String LOG_ERR_0008 = "ERR.003.014.0008"; //$NON-NLS-1$
		public static final String LOG_ERR_0009 = "ERR.003.014.0009"; //$NON-NLS-1$
		public static final String LOG_ERR_0010 = "ERR.003.014.0010"; //$NON-NLS-1$
		public static final String LOG_ERR_0011 = "ERR.003.014.0011"; //$NON-NLS-1$
		public static final String LOG_ERR_0012 = "ERR.003.014.0012"; //$NON-NLS-1$
		public static final String LOG_ERR_0013 = "ERR.003.014.0013"; //$NON-NLS-1$
		public static final String LOG_ERR_0014 = "ERR.003.014.0014"; //$NON-NLS-1$
		public static final String LOG_ERR_0015 = "ERR.003.014.0015"; //$NON-NLS-1$
		public static final String LOG_ERR_0016 = "ERR.003.014.0016"; //$NON-NLS-1$
		public static final String LOG_ERR_0017 = "ERR.003.014.0017"; //$NON-NLS-1$
		public static final String LOG_ERR_0018 = "ERR.003.014.0018"; //$NON-NLS-1$
		public static final String LOG_ERR_0019 = "ERR.003.014.0019"; //$NON-NLS-1$
		public static final String LOG_ERR_0020 = "ERR.003.014.0020"; //$NON-NLS-1$
		public static final String LOG_ERR_0021 = "ERR.003.014.0021"; //$NON-NLS-1$
		public static final String LOG_ERR_0022 = "ERR.003.014.0022"; //$NON-NLS-1$
		public static final String LOG_ERR_0023 = "ERR.003.014.0023"; //$NON-NLS-1$
		public static final String LOG_ERR_0024 = "ERR.003.014.0024"; //$NON-NLS-1$
		public static final String LOG_ERR_0025 = "ERR.003.014.0025"; //$NON-NLS-1$
		public static final String LOG_ERR_0026 = "ERR.003.014.0026"; //$NON-NLS-1$
		public static final String LOG_ERR_0027 = "ERR.003.014.0027"; //$NON-NLS-1$
		public static final String LOG_ERR_0028 = "ERR.003.014.0028"; //$NON-NLS-1$
		public static final String LOG_ERR_0029 = "ERR.003.014.0029"; //$NON-NLS-1$
		public static final String LOG_ERR_0030 = "ERR.003.014.0030"; //$NON-NLS-1$
		public static final String LOG_ERR_0031 = "ERR.003.014.0031"; //$NON-NLS-1$
        public static final String LOG_ERR_0032 = "ERR.003.014.0032"; //$NON-NLS-1$

		/** jdbc (015) */
		public static final String JDBC_ERR_0001 = "ERR.003.015.0001"; //$NON-NLS-1$
		public static final String JDBC_ERR_0002 = "ERR.003.015.0002"; //$NON-NLS-1$
		public static final String JDBC_ERR_0003 = "ERR.003.015.0003"; //$NON-NLS-1$
		public static final String JDBC_ERR_0004 = "ERR.003.015.0004"; //$NON-NLS-1$
		public static final String JDBC_ERR_0005 = "ERR.003.015.0005"; //$NON-NLS-1$
		public static final String JDBC_ERR_0006 = "ERR.003.015.0006"; //$NON-NLS-1$
		public static final String JDBC_ERR_0007 = "ERR.003.015.0007"; //$NON-NLS-1$
		public static final String JDBC_ERR_0008 = "ERR.003.015.0008"; //$NON-NLS-1$
		public static final String JDBC_ERR_0009 = "ERR.003.015.0009"; //$NON-NLS-1$
		public static final String JDBC_ERR_0010 = "ERR.003.015.0010"; //$NON-NLS-1$
		public static final String JDBC_ERR_0011 = "ERR.003.015.0011"; //$NON-NLS-1$
		public static final String JDBC_ERR_0012 = "ERR.003.015.0012"; //$NON-NLS-1$
		public static final String JDBC_ERR_0013 = "ERR.003.015.0013"; //$NON-NLS-1$
		public static final String JDBC_ERR_0014 = "ERR.003.015.0014"; //$NON-NLS-1$
		public static final String JDBC_ERR_0015 = "ERR.003.015.0015"; //$NON-NLS-1$
		public static final String JDBC_ERR_0016 = "ERR.003.015.0016"; //$NON-NLS-1$
		public static final String JDBC_ERR_0017 = "ERR.003.015.0017"; //$NON-NLS-1$
		public static final String JDBC_ERR_0018 = "ERR.003.015.0018"; //$NON-NLS-1$
		public static final String JDBC_ERR_0019 = "ERR.003.015.0019"; //$NON-NLS-1$
		public static final String JDBC_ERR_0020 = "ERR.003.015.0020"; //$NON-NLS-1$
		public static final String JDBC_ERR_0021 = "ERR.003.015.0021"; //$NON-NLS-1$
		public static final String JDBC_ERR_0022 = "ERR.003.015.0022"; //$NON-NLS-1$
		public static final String JDBC_ERR_0023 = "ERR.003.015.0023"; //$NON-NLS-1$
		public static final String JDBC_ERR_0024 = "ERR.003.015.0024"; //$NON-NLS-1$
		public static final String JDBC_ERR_0025 = "ERR.003.015.0025"; //$NON-NLS-1$
		public static final String JDBC_ERR_0026 = "ERR.003.015.0026"; //$NON-NLS-1$
		public static final String JDBC_ERR_0027 = "ERR.003.015.0027"; //$NON-NLS-1$
		public static final String JDBC_ERR_0028 = "ERR.003.015.0028"; //$NON-NLS-1$
		public static final String JDBC_ERR_0029 = "ERR.003.015.0029"; //$NON-NLS-1$
		public static final String JDBC_ERR_0030 = "ERR.003.015.0030"; //$NON-NLS-1$
		public static final String JDBC_ERR_0031 = "ERR.003.015.0031"; //$NON-NLS-1$
		public static final String JDBC_ERR_0032 = "ERR.003.015.0032"; //$NON-NLS-1$
		public static final String JDBC_ERR_0033 = "ERR.003.015.0033"; //$NON-NLS-1$
		public static final String JDBC_ERR_0034 = "ERR.003.015.0034"; //$NON-NLS-1$
		public static final String JDBC_ERR_0035 = "ERR.003.015.0035"; //$NON-NLS-1$
		public static final String JDBC_ERR_0036 = "ERR.003.015.0036"; //$NON-NLS-1$
		public static final String JDBC_ERR_0037 = "ERR.003.015.0037"; //$NON-NLS-1$
		public static final String JDBC_ERR_0038 = "ERR.003.015.0038"; //$NON-NLS-1$
		public static final String JDBC_ERR_0039 = "ERR.003.015.0039"; //$NON-NLS-1$
		public static final String JDBC_ERR_0040 = "ERR.003.015.0040"; //$NON-NLS-1$
		public static final String JDBC_ERR_0041 = "ERR.003.015.0041"; //$NON-NLS-1$
		public static final String JDBC_ERR_0042 = "ERR.003.015.0042"; //$NON-NLS-1$
		public static final String JDBC_ERR_0043 = "ERR.003.015.0043"; //$NON-NLS-1$
		public static final String JDBC_ERR_0044 = "ERR.003.015.0044"; //$NON-NLS-1$
		public static final String JDBC_ERR_0045 = "ERR.003.015.0045"; //$NON-NLS-1$
		public static final String JDBC_ERR_0046 = "ERR.003.015.0046"; //$NON-NLS-1$
		public static final String JDBC_ERR_0047 = "ERR.003.015.0047"; //$NON-NLS-1$
		public static final String JDBC_ERR_0048 = "ERR.003.015.0048"; //$NON-NLS-1$
		public static final String JDBC_ERR_0049 = "ERR.003.015.0049"; //$NON-NLS-1$
		public static final String JDBC_ERR_0050 = "ERR.003.015.0050"; //$NON-NLS-1$
		public static final String JDBC_ERR_0051 = "ERR.003.015.0051"; //$NON-NLS-1$
		public static final String JDBC_ERR_0052 = "ERR.003.015.0052"; //$NON-NLS-1$
		public static final String JDBC_ERR_0053 = "ERR.003.015.0053"; //$NON-NLS-1$
		public static final String JDBC_ERR_0054 = "ERR.003.015.0054"; //$NON-NLS-1$
		public static final String JDBC_ERR_0055 = "ERR.003.015.0055"; //$NON-NLS-1$
		public static final String JDBC_ERR_0056 = "ERR.003.015.0056"; //$NON-NLS-1$
		public static final String JDBC_ERR_0057 = "ERR.003.015.0057"; //$NON-NLS-1$
		public static final String JDBC_ERR_0058 = "ERR.003.015.0058"; //$NON-NLS-1$
		public static final String JDBC_ERR_0059 = "ERR.003.015.0059"; //$NON-NLS-1$
		public static final String JDBC_ERR_0060 = "ERR.003.015.0060"; //$NON-NLS-1$
		public static final String JDBC_ERR_0061 = "ERR.003.015.0061"; //$NON-NLS-1$
		public static final String JDBC_ERR_0062 = "ERR.003.015.0062"; //$NON-NLS-1$
		public static final String JDBC_ERR_0063 = "ERR.003.015.0063"; //$NON-NLS-1$
		public static final String JDBC_ERR_0064 = "ERR.003.015.0064"; //$NON-NLS-1$
        public static final String JDBC_ERR_0065 = "ERR.003.015.0065"; //$NON-NLS-1$


		/** messaging (017) */
		public static final String MESSAGING_ERR_0001 = "ERR.003.017.0001"; //$NON-NLS-1$
		public static final String MESSAGING_ERR_0002 = "ERR.003.017.0002"; //$NON-NLS-1$
		public static final String MESSAGING_ERR_0003 = "ERR.003.017.0003"; //$NON-NLS-1$
		public static final String MESSAGING_ERR_0004 = "ERR.003.017.0004"; //$NON-NLS-1$
		public static final String MESSAGING_ERR_0005 = "ERR.003.017.0005"; //$NON-NLS-1$
		public static final String MESSAGING_ERR_0006 = "ERR.003.017.0006"; //$NON-NLS-1$
		public static final String MESSAGING_ERR_0007 = "ERR.003.017.0007"; //$NON-NLS-1$
		public static final String MESSAGING_ERR_0008 = "ERR.003.017.0008"; //$NON-NLS-1$
		public static final String MESSAGING_ERR_0009 = "ERR.003.017.0009"; //$NON-NLS-1$
		public static final String MESSAGING_ERR_0010 = "ERR.003.017.0010"; //$NON-NLS-1$
		public static final String MESSAGING_ERR_0011 = "ERR.003.017.0011"; //$NON-NLS-1$
		public static final String MESSAGING_ERR_0012 = "ERR.003.017.0012"; //$NON-NLS-1$
		public static final String MESSAGING_ERR_0013 = "ERR.003.017.0013"; //$NON-NLS-1$
		public static final String MESSAGING_ERR_0014 = "ERR.003.017.0014"; //$NON-NLS-1$
		public static final String MESSAGING_ERR_0015 = "ERR.003.017.0015"; //$NON-NLS-1$

		/** namedobject (018) */
		public static final String NAMEDOBJECT_ERR_0001 = "ERR.003.018.0001"; //$NON-NLS-1$
		public static final String NAMEDOBJECT_ERR_0002 = "ERR.003.018.0002"; //$NON-NLS-1$
		public static final String NAMEDOBJECT_ERR_0003 = "ERR.003.018.0003"; //$NON-NLS-1$
		public static final String NAMEDOBJECT_ERR_0004 = "ERR.003.018.0004"; //$NON-NLS-1$
		public static final String NAMEDOBJECT_ERR_0005 = "ERR.003.018.0005"; //$NON-NLS-1$
		public static final String NAMEDOBJECT_ERR_0006 = "ERR.003.018.0006"; //$NON-NLS-1$
		public static final String NAMEDOBJECT_ERR_0007 = "ERR.003.018.0007"; //$NON-NLS-1$

				/** object (019) */
		public static final String OBJECT_ERR_0001 = "ERR.003.019.0001"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0002 = "ERR.003.019.0002"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0003 = "ERR.003.019.0003"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0004 = "ERR.003.019.0004"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0005 = "ERR.003.019.0005"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0006 = "ERR.003.019.0006"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0007 = "ERR.003.019.0007"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0008 = "ERR.003.019.0008"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0009 = "ERR.003.019.0009"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0010 = "ERR.003.019.0010"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0011 = "ERR.003.019.0011"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0012 = "ERR.003.019.0012"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0013 = "ERR.003.019.0013"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0014 = "ERR.003.019.0014"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0015 = "ERR.003.019.0015"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0016 = "ERR.003.019.0016"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0017 = "ERR.003.019.0017"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0018 = "ERR.003.019.0018"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0019 = "ERR.003.019.0019"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0020 = "ERR.003.019.0020"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0021 = "ERR.003.019.0021"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0022 = "ERR.003.019.0022"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0023 = "ERR.003.019.0023"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0024 = "ERR.003.019.0024"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0025 = "ERR.003.019.0025"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0026 = "ERR.003.019.0026"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0027 = "ERR.003.019.0027"; //$NON-NLS-1$
		public static final String OBJECT_ERR_0028 = "ERR.003.019.0028"; //$NON-NLS-1$

		/** properties (021) */
		public static final String PROPERTIES_ERR_0001 = "ERR.003.021.0001"; //$NON-NLS-1$
		public static final String PROPERTIES_ERR_0002 = "ERR.003.021.0002"; //$NON-NLS-1$
		public static final String PROPERTIES_ERR_0003 = "ERR.003.021.0003"; //$NON-NLS-1$
		public static final String PROPERTIES_ERR_0004 = "ERR.003.021.0004"; //$NON-NLS-1$
		public static final String PROPERTIES_ERR_0005 = "ERR.003.021.0005"; //$NON-NLS-1$
		public static final String PROPERTIES_ERR_0006 = "ERR.003.021.0006"; //$NON-NLS-1$
		public static final String PROPERTIES_ERR_0007 = "ERR.003.021.0007"; //$NON-NLS-1$
		public static final String PROPERTIES_ERR_0008 = "ERR.003.021.0008"; //$NON-NLS-1$
		public static final String PROPERTIES_ERR_0009 = "ERR.003.021.0009"; //$NON-NLS-1$
		public static final String PROPERTIES_ERR_0012 = "ERR.003.021.0012"; //$NON-NLS-1$

		/** proxy (022) */
		public static final String PROXY_ERR_0001 = "ERR.003.022.0001"; //$NON-NLS-1$
		public static final String PROXY_ERR_0002 = "ERR.003.022.0002"; //$NON-NLS-1$
		public static final String PROXY_ERR_0003 = "ERR.003.022.0003"; //$NON-NLS-1$
		public static final String PROXY_ERR_0004 = "ERR.003.022.0004"; //$NON-NLS-1$
		public static final String PROXY_ERR_0005 = "ERR.003.022.0005"; //$NON-NLS-1$

		/** queue (023) */
		public static final String QUEUE_ERR_0001 = "ERR.003.023.0001"; //$NON-NLS-1$
		public static final String QUEUE_ERR_0002 = "ERR.003.023.0002"; //$NON-NLS-1$
		public static final String QUEUE_ERR_0003 = "ERR.003.023.0003"; //$NON-NLS-1$
		public static final String QUEUE_ERR_0004 = "ERR.003.023.0004"; //$NON-NLS-1$

		/** remote (024) */

		/** thread (025) */
		public static final String THREAD_ERR_0001 = "ERR.003.025.0001"; //$NON-NLS-1$
		public static final String THREAD_ERR_0002 = "ERR.003.025.0002"; //$NON-NLS-1$

		/** transaction (026) */
		public static final String TRANSACTION_ERR_0001 = "ERR.003.026.0001"; //$NON-NLS-1$

		/** transform (027) */
		public static final String TRANSFORM_ERR_0001 = "ERR.003.027.0001"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0002 = "ERR.003.027.0002"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0003 = "ERR.003.027.0003"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0004 = "ERR.003.027.0004"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0005 = "ERR.003.027.0005"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0006 = "ERR.003.027.0006"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0007 = "ERR.003.027.0007"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0008 = "ERR.003.027.0008"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0009 = "ERR.003.027.0009"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0010 = "ERR.003.027.0010"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0011 = "ERR.003.027.0011"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0012 = "ERR.003.027.0012"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0013 = "ERR.003.027.0013"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0014 = "ERR.003.027.0014"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0015 = "ERR.003.027.0015"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0016 = "ERR.003.027.0016"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0017 = "ERR.003.027.0017"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0018 = "ERR.003.027.0018"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0019 = "ERR.003.027.0019"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0020 = "ERR.003.027.0020"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0021 = "ERR.003.027.0021"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0022 = "ERR.003.027.0022"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0023 = "ERR.003.027.0023"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0024 = "ERR.003.027.0024"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0025 = "ERR.003.027.0025"; //$NON-NLS-1$
		public static final String TRANSFORM_ERR_0026 = "ERR.003.027.0026"; //$NON-NLS-1$

		/** tree (028) */
		public static final String TREE_ERR_0001 = "ERR.003.028.0001"; //$NON-NLS-1$
		public static final String TREE_ERR_0002 = "ERR.003.028.0002"; //$NON-NLS-1$
		public static final String TREE_ERR_0003 = "ERR.003.028.0003"; //$NON-NLS-1$
		public static final String TREE_ERR_0004 = "ERR.003.028.0004"; //$NON-NLS-1$
		public static final String TREE_ERR_0005 = "ERR.003.028.0005"; //$NON-NLS-1$
		public static final String TREE_ERR_0006 = "ERR.003.028.0006"; //$NON-NLS-1$
		public static final String TREE_ERR_0007 = "ERR.003.028.0007"; //$NON-NLS-1$
		public static final String TREE_ERR_0008 = "ERR.003.028.0008"; //$NON-NLS-1$
		public static final String TREE_ERR_0009 = "ERR.003.028.0009"; //$NON-NLS-1$
		public static final String TREE_ERR_0010 = "ERR.003.028.0010"; //$NON-NLS-1$
		public static final String TREE_ERR_0011 = "ERR.003.028.0011"; //$NON-NLS-1$
		public static final String TREE_ERR_0012 = "ERR.003.028.0012"; //$NON-NLS-1$
		public static final String TREE_ERR_0013 = "ERR.003.028.0013"; //$NON-NLS-1$
		public static final String TREE_ERR_0014 = "ERR.003.028.0014"; //$NON-NLS-1$
		public static final String TREE_ERR_0015 = "ERR.003.028.0015"; //$NON-NLS-1$
		public static final String TREE_ERR_0016 = "ERR.003.028.0016"; //$NON-NLS-1$
		public static final String TREE_ERR_0017 = "ERR.003.028.0017"; //$NON-NLS-1$
		public static final String TREE_ERR_0018 = "ERR.003.028.0018"; //$NON-NLS-1$
		public static final String TREE_ERR_0019 = "ERR.003.028.0019"; //$NON-NLS-1$
		public static final String TREE_ERR_0020 = "ERR.003.028.0020"; //$NON-NLS-1$
		public static final String TREE_ERR_0021 = "ERR.003.028.0021"; //$NON-NLS-1$
		public static final String TREE_ERR_0022 = "ERR.003.028.0022"; //$NON-NLS-1$
		public static final String TREE_ERR_0023 = "ERR.003.028.0023"; //$NON-NLS-1$
		public static final String TREE_ERR_0024 = "ERR.003.028.0024"; //$NON-NLS-1$
		public static final String TREE_ERR_0025 = "ERR.003.028.0025"; //$NON-NLS-1$
		public static final String TREE_ERR_0026 = "ERR.003.028.0026"; //$NON-NLS-1$
		public static final String TREE_ERR_0027 = "ERR.003.028.0027"; //$NON-NLS-1$
		public static final String TREE_ERR_0028 = "ERR.003.028.0028"; //$NON-NLS-1$
		public static final String TREE_ERR_0029 = "ERR.003.028.0029"; //$NON-NLS-1$
		public static final String TREE_ERR_0030 = "ERR.003.028.0030"; //$NON-NLS-1$
		public static final String TREE_ERR_0031 = "ERR.003.028.0031"; //$NON-NLS-1$
		public static final String TREE_ERR_0032 = "ERR.003.028.0032"; //$NON-NLS-1$
		public static final String TREE_ERR_0033 = "ERR.003.028.0033"; //$NON-NLS-1$
		public static final String TREE_ERR_0034 = "ERR.003.028.0034"; //$NON-NLS-1$
		public static final String TREE_ERR_0035 = "ERR.003.028.0035"; //$NON-NLS-1$
		public static final String TREE_ERR_0036 = "ERR.003.028.0036"; //$NON-NLS-1$
		public static final String TREE_ERR_0037 = "ERR.003.028.0037"; //$NON-NLS-1$
		public static final String TREE_ERR_0038 = "ERR.003.028.0038"; //$NON-NLS-1$
		public static final String TREE_ERR_0039 = "ERR.003.028.0039"; //$NON-NLS-1$
		public static final String TREE_ERR_0040 = "ERR.003.028.0040"; //$NON-NLS-1$
		public static final String TREE_ERR_0041 = "ERR.003.028.0041"; //$NON-NLS-1$
		public static final String TREE_ERR_0042 = "ERR.003.028.0042"; //$NON-NLS-1$
		public static final String TREE_ERR_0043 = "ERR.003.028.0043"; //$NON-NLS-1$
		public static final String TREE_ERR_0044 = "ERR.003.028.0044"; //$NON-NLS-1$
		public static final String TREE_ERR_0045 = "ERR.003.028.0045"; //$NON-NLS-1$
		public static final String TREE_ERR_0046 = "ERR.003.028.0046"; //$NON-NLS-1$
		public static final String TREE_ERR_0047 = "ERR.003.028.0047"; //$NON-NLS-1$
		public static final String TREE_ERR_0048 = "ERR.003.028.0048"; //$NON-NLS-1$
		public static final String TREE_ERR_0049 = "ERR.003.028.0049"; //$NON-NLS-1$
		public static final String TREE_ERR_0050 = "ERR.003.028.0050"; //$NON-NLS-1$
		public static final String TREE_ERR_0051 = "ERR.003.028.0051"; //$NON-NLS-1$
		public static final String TREE_ERR_0052 = "ERR.003.028.0052"; //$NON-NLS-1$
		public static final String TREE_ERR_0053 = "ERR.003.028.0053"; //$NON-NLS-1$
		public static final String TREE_ERR_0054 = "ERR.003.028.0054"; //$NON-NLS-1$
		public static final String TREE_ERR_0055 = "ERR.003.028.0055"; //$NON-NLS-1$
		public static final String TREE_ERR_0056 = "ERR.003.028.0056"; //$NON-NLS-1$
		public static final String TREE_ERR_0057 = "ERR.003.028.0057"; //$NON-NLS-1$
		public static final String TREE_ERR_0058 = "ERR.003.028.0058"; //$NON-NLS-1$
		public static final String TREE_ERR_0059 = "ERR.003.028.0059"; //$NON-NLS-1$
		public static final String TREE_ERR_0060 = "ERR.003.028.0060"; //$NON-NLS-1$
		public static final String TREE_ERR_0061 = "ERR.003.028.0061"; //$NON-NLS-1$
		public static final String TREE_ERR_0062 = "ERR.003.028.0062"; //$NON-NLS-1$
		public static final String TREE_ERR_0063 = "ERR.003.028.0063"; //$NON-NLS-1$
		public static final String TREE_ERR_0064 = "ERR.003.028.0064"; //$NON-NLS-1$
		public static final String TREE_ERR_0065 = "ERR.003.028.0065"; //$NON-NLS-1$
		public static final String TREE_ERR_0066 = "ERR.003.028.0066"; //$NON-NLS-1$
		public static final String TREE_ERR_0067 = "ERR.003.028.0067"; //$NON-NLS-1$
		public static final String TREE_ERR_0068 = "ERR.003.028.0068"; //$NON-NLS-1$
		public static final String TREE_ERR_0069 = "ERR.003.028.0069"; //$NON-NLS-1$
		public static final String TREE_ERR_0070 = "ERR.003.028.0070"; //$NON-NLS-1$
		public static final String TREE_ERR_0071 = "ERR.003.028.0071"; //$NON-NLS-1$
		public static final String TREE_ERR_0072 = "ERR.003.028.0072"; //$NON-NLS-1$



		/** util (030) */
		public static final String CM_UTIL_ERR_0001 = "ERR.003.030.0001"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0002 = "ERR.003.030.0002"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0003 = "ERR.003.030.0003"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0004 = "ERR.003.030.0004"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0005 = "ERR.003.030.0005"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0006 = "ERR.003.030.0006"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0007 = "ERR.003.030.0007"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0008 = "ERR.003.030.0008"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0009 = "ERR.003.030.0009"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0010 = "ERR.003.030.0010"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0011 = "ERR.003.030.0011"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0012 = "ERR.003.030.0012"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0013 = "ERR.003.030.0013"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0014 = "ERR.003.030.0014"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0015 = "ERR.003.030.0015"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0016 = "ERR.003.030.0016"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0017 = "ERR.003.030.0017"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0018 = "ERR.003.030.0018"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0019 = "ERR.003.030.0019"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0020 = "ERR.003.030.0020"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0021 = "ERR.003.030.0021"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0022 = "ERR.003.030.0022"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0023 = "ERR.003.030.0023"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0024 = "ERR.003.030.0024"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0025 = "ERR.003.030.0025"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0026 = "ERR.003.030.0026"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0027 = "ERR.003.030.0027"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0028 = "ERR.003.030.0028"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0029 = "ERR.003.030.0029"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0030 = "ERR.003.030.0030"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0031 = "ERR.003.030.0031"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0032 = "ERR.003.030.0032"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0033 = "ERR.003.030.0033"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0034 = "ERR.003.030.0034"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0035 = "ERR.003.030.0035"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0036 = "ERR.003.030.0036"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0037 = "ERR.003.030.0037"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0038 = "ERR.003.030.0038"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0039 = "ERR.003.030.0039"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0040 = "ERR.003.030.0040"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0041 = "ERR.003.030.0041"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0042 = "ERR.003.030.0042"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0043 = "ERR.003.030.0043"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0044 = "ERR.003.030.0044"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0045 = "ERR.003.030.0045"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0046 = "ERR.003.030.0046"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0047 = "ERR.003.030.0047"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0048 = "ERR.003.030.0048"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0049 = "ERR.003.030.0049"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0050 = "ERR.003.030.0050"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0051 = "ERR.003.030.0051"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0052 = "ERR.003.030.0052"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0053 = "ERR.003.030.0053"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0054 = "ERR.003.030.0054"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0055 = "ERR.003.030.0055"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0056 = "ERR.003.030.0056"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0057 = "ERR.003.030.0057"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0058 = "ERR.003.030.0058"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0059 = "ERR.003.030.0059"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0060 = "ERR.003.030.0060"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0061 = "ERR.003.030.0061"; //$NON-NLS-1$
		
		public static final String CM_UTIL_ERR_0063 = "ERR.003.030.0063"; //$NON-NLS-1$
		
		public static final String CM_UTIL_ERR_0065 = "ERR.003.030.0065"; //$NON-NLS-1$
		
		
		public static final String CM_UTIL_ERR_0069 = "ERR.003.030.0069"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0070 = "ERR.003.030.0070"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0079 = "ERR.003.030.0079"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0080 = "ERR.003.030.0080"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0082 = "ERR.003.030.0082"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0083 = "ERR.003.030.0083"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0084 = "ERR.003.030.0084"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0085 = "ERR.003.030.0085"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0086 = "ERR.003.030.0086"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0087 = "ERR.003.030.0087"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0088 = "ERR.003.030.0088"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0089 = "ERR.003.030.0089"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0090 = "ERR.003.030.0090"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0091 = "ERR.003.030.0091"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0092 = "ERR.003.030.0092"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0093 = "ERR.003.030.0093"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0094 = "ERR.003.030.0094"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0095 = "ERR.003.030.0095"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0096 = "ERR.003.030.0096"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0097 = "ERR.003.030.0097"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0098 = "ERR.003.030.0098"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0099 = "ERR.003.030.0099"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0100 = "ERR.003.030.0100"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0101 = "ERR.003.030.0101"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0102 = "ERR.003.030.0102"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0103 = "ERR.003.030.0103"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0104 = "ERR.003.030.0104"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0105 = "ERR.003.030.0105"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0106 = "ERR.003.030.0106"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0107 = "ERR.003.030.0107"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0108 = "ERR.003.030.0108"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0109 = "ERR.003.030.0109"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0110 = "ERR.003.030.0110"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0111 = "ERR.003.030.0111"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0112 = "ERR.003.030.0112"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0150 = "ERR.003.030.0150"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0151 = "ERR.003.030.0151"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0152 = "ERR.003.030.0152"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0153 = "ERR.003.030.0153"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0154 = "ERR.003.030.0154"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0155 = "ERR.003.030.0155"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0156 = "ERR.003.030.0156"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0157 = "ERR.003.030.0157"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0158 = "ERR.003.030.0158"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0159 = "ERR.003.030.0159"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0160 = "ERR.003.030.0160"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0161 = "ERR.003.030.0161"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0162 = "ERR.003.030.0162"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0163 = "ERR.003.030.0163"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0164 = "ERR.003.030.0164"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0165 = "ERR.003.030.0165"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0166 = "ERR.003.030.0166"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0167 = "ERR.003.030.0167"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0168 = "ERR.003.030.0168"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0169 = "ERR.003.030.0169"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0170 = "ERR.003.030.0170"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0171 = "ERR.003.030.0171"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0172 = "ERR.003.030.0172"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0173 = "ERR.003.030.0173"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0174 = "ERR.003.030.0174"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0175 = "ERR.003.030.0175"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0176 = "ERR.003.030.0176"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0177 = "ERR.003.030.0177"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0178 = "ERR.003.030.0178"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0179 = "ERR.003.030.0179"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0180 = "ERR.003.030.0180"; //$NON-NLS-1$
		public static final String CM_UTIL_ERR_0181 = "ERR.003.030.0181"; //$NON-NLS-1$
        public static final String CM_UTIL_ERR_0182 = "ERR.003.030.0182"; //$NON-NLS-1$
        public static final String CM_UTIL_ERR_0183 = "ERR.003.030.0183"; //$NON-NLS-1$
        public static final String CM_UTIL_ERR_0184 = "ERR.003.030.0184"; //$NON-NLS-1$
        
		/** xml (032) */
		public static final String XML_ERR_0001 = "ERR.003.032.0001"; //$NON-NLS-1$
		public static final String XML_ERR_0002 = "ERR.003.032.0002"; //$NON-NLS-1$
		public static final String XML_ERR_0003 = "ERR.003.032.0003"; //$NON-NLS-1$
		public static final String XML_ERR_0004 = "ERR.003.032.0004"; //$NON-NLS-1$
		public static final String XML_ERR_0005 = "ERR.003.032.0005"; //$NON-NLS-1$
		public static final String XML_ERR_0006 = "ERR.003.032.0006"; //$NON-NLS-1$
		public static final String XML_ERR_0007 = "ERR.003.032.0007"; //$NON-NLS-1$
		public static final String XML_ERR_0008 = "ERR.003.032.0008"; //$NON-NLS-1$
		public static final String XML_ERR_0009 = "ERR.003.032.0009"; //$NON-NLS-1$
		public static final String XML_ERR_0010 = "ERR.003.032.0010"; //$NON-NLS-1$
		public static final String XML_ERR_0011 = "ERR.003.032.0011"; //$NON-NLS-1$
		public static final String XML_ERR_0012 = "ERR.003.032.0012"; //$NON-NLS-1$
		public static final String XML_ERR_0013 = "ERR.003.032.0013"; //$NON-NLS-1$
		public static final String XML_ERR_0014 = "ERR.003.032.0014"; //$NON-NLS-1$
		public static final String XML_ERR_0015 = "ERR.003.032.0015"; //$NON-NLS-1$
		public static final String XML_ERR_0016 = "ERR.003.032.0016"; //$NON-NLS-1$

		/** pooling (033) */
        
        /** extension package (004) */
        public static final String EXTENSION_0001 = "ERR.014.004.0001"; //$NON-NLS-1$
        public static final String EXTENSION_0002 = "ERR.014.004.0002"; //$NON-NLS-1$
        public static final String EXTENSION_0003 = "ERR.014.004.0003"; //$NON-NLS-1$
        public static final String EXTENSION_0004 = "ERR.014.004.0004"; //$NON-NLS-1$
        public static final String EXTENSION_0005 = "ERR.014.004.0005"; //$NON-NLS-1$
        public static final String EXTENSION_0006 = "ERR.014.004.0006"; //$NON-NLS-1$
        public static final String EXTENSION_0007 = "ERR.014.004.0007"; //$NON-NLS-1$
        public static final String EXTENSION_0008 = "ERR.014.004.0008"; //$NON-NLS-1$
        public static final String EXTENSION_0009 = "ERR.014.004.0009"; //$NON-NLS-1$
        public static final String EXTENSION_0010 = "ERR.014.004.0010"; //$NON-NLS-1$
        public static final String EXTENSION_0011 = "ERR.014.004.0011"; //$NON-NLS-1$
        public static final String EXTENSION_0012 = "ERR.014.004.0012"; //$NON-NLS-1$
        public static final String EXTENSION_0013 = "ERR.014.004.0013"; //$NON-NLS-1$
        public static final String EXTENSION_0014 = "ERR.014.004.0014"; //$NON-NLS-1$
        public static final String EXTENSION_0015 = "ERR.014.004.0015"; //$NON-NLS-1$
        public static final String EXTENSION_0016 = "ERR.014.004.0016"; //$NON-NLS-1$
        public static final String EXTENSION_0017 = "ERR.014.004.0017"; //$NON-NLS-1$
        public static final String EXTENSION_0018 = "ERR.014.004.0018"; //$NON-NLS-1$
        public static final String EXTENSION_0019 = "ERR.014.004.0019"; //$NON-NLS-1$
        public static final String EXTENSION_0020 = "ERR.014.004.0020"; //$NON-NLS-1$
        public static final String EXTENSION_0021 = "ERR.014.004.0021"; //$NON-NLS-1$
        public static final String EXTENSION_0022 = "ERR.014.004.0022"; //$NON-NLS-1$
        public static final String EXTENSION_0023 = "ERR.014.004.0023"; //$NON-NLS-1$
        public static final String EXTENSION_0024 = "ERR.014.004.0024"; //$NON-NLS-1$
        public static final String EXTENSION_0025 = "ERR.014.004.0025"; //$NON-NLS-1$
        public static final String EXTENSION_0026 = "ERR.014.004.0026"; //$NON-NLS-1$
        public static final String EXTENSION_0027 = "ERR.014.004.0027"; //$NON-NLS-1$
        public static final String EXTENSION_0028 = "ERR.014.004.0028"; //$NON-NLS-1$
        public static final String EXTENSION_0029 = "ERR.014.004.0029"; //$NON-NLS-1$
        public static final String EXTENSION_0030 = "ERR.014.004.0030"; //$NON-NLS-1$
        public static final String EXTENSION_0031 = "ERR.014.004.0031"; //$NON-NLS-1$
        public static final String EXTENSION_0032 = "ERR.014.004.0032"; //$NON-NLS-1$
        public static final String EXTENSION_0033 = "ERR.014.004.0033"; //$NON-NLS-1$
        public static final String EXTENSION_0034 = "ERR.014.004.0034"; //$NON-NLS-1$
        public static final String EXTENSION_0035 = "ERR.014.004.0035"; //$NON-NLS-1$
        public static final String EXTENSION_0036 = "ERR.014.004.0036"; //$NON-NLS-1$
        public static final String EXTENSION_0037 = "ERR.014.004.0037"; //$NON-NLS-1$
        public static final String EXTENSION_0038 = "ERR.014.004.0038"; //$NON-NLS-1$
        public static final String EXTENSION_0039 = "ERR.014.004.0039"; //$NON-NLS-1$
        public static final String EXTENSION_0040 = "ERR.014.004.0040"; //$NON-NLS-1$
        public static final String EXTENSION_0041 = "ERR.014.004.0041"; //$NON-NLS-1$
        public static final String EXTENSION_0042 = "ERR.014.004.0042"; //$NON-NLS-1$
        public static final String EXTENSION_0043 = "ERR.014.004.0043"; //$NON-NLS-1$
        public static final String EXTENSION_0044 = "ERR.014.004.0044"; //$NON-NLS-1$
        public static final String EXTENSION_0045 = "ERR.014.004.0045"; //$NON-NLS-1$
        public static final String EXTENSION_0046 = "ERR.014.004.0046"; //$NON-NLS-1$
        public static final String EXTENSION_0047 = "ERR.014.004.0047"; //$NON-NLS-1$
        public static final String EXTENSION_0048 = "ERR.014.004.0048"; //$NON-NLS-1$
        public static final String EXTENSION_0049 = "ERR.014.004.0049"; //$NON-NLS-1$
        public static final String EXTENSION_0050 = "ERR.014.004.0050"; //$NON-NLS-1$
        public static final String EXTENSION_0051 = "ERR.014.004.0051"; //$NON-NLS-1$
        public static final String EXTENSION_0052 = "ERR.014.004.0052"; //$NON-NLS-1$
        public static final String EXTENSION_0053 = "ERR.014.004.0053"; //$NON-NLS-1$
        public static final String EXTENSION_0054 = "ERR.014.004.0054"; //$NON-NLS-1$
        public static final String EXTENSION_0055 = "ERR.014.004.0055"; //$NON-NLS-1$
        public static final String EXTENSION_0056 = "ERR.014.004.0056"; //$NON-NLS-1$
        public static final String EXTENSION_0057 = "ERR.014.004.0057"; //$NON-NLS-1$
        public static final String EXTENSION_0058 = "ERR.014.004.0058"; //$NON-NLS-1$
        public static final String EXTENSION_0059 = "ERR.014.004.0059"; //$NON-NLS-1$
        public static final String EXTENSION_0060 = "ERR.014.004.0060"; //$NON-NLS-1$
        public static final String EXTENSION_0061 = "ERR.014.004.0061"; //$NON-NLS-1$
        public static final String EXTENSION_0062 = "ERR.014.004.0062"; //$NON-NLS-1$
        public static final String EXTENSION_0063 = "ERR.014.004.0063"; //$NON-NLS-1$
        public static final String EXTENSION_0064 = "ERR.014.004.0064"; //$NON-NLS-1$
        public static final String EXTENSION_0065 = "ERR.014.004.0065"; //$NON-NLS-1$
        public static final String EXTENSION_0066 = "ERR.014.004.0066"; //$NON-NLS-1$
        public static final String EXTENSION_0067 = "ERR.014.004.0067"; //$NON-NLS-1$
        public static final String EXTENSION_0068 = "ERR.014.004.0068"; //$NON-NLS-1$
        public static final String EXTENSION_0069 = "ERR.014.004.0069"; //$NON-NLS-1$
        public static final String EXTENSION_0070 = "ERR.014.004.0070"; //$NON-NLS-1$
        public static final String EXTENSION_0071 = "ERR.014.004.0071"; //$NON-NLS-1$
        public static final String EXTENSION_0072 = "ERR.014.004.0072"; //$NON-NLS-1$
        public static final String EXTENSION_0073 = "ERR.014.004.0073"; //$NON-NLS-1$
    
        
        
       
}

