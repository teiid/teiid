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
package org.teiid.example;

import java.awt.Container;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;

import javax.swing.AbstractAction;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;

public class TeiidExamplesApp extends JFrame {

	private static final long serialVersionUID = -2006764910415872215L;
	
	public TeiidExamplesApp() {
		super("JBoss Teiid examples");
        setContentPane(createContentPane());
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	}

	@SuppressWarnings("serial")
	private Container createContentPane() {
		JPanel contentPane = new JPanel(new GridLayout(0, 1));
		
		contentPane.add(new JLabel("Basic Examples"));
		
		contentPane.add(new JButton(new AbstractAction("Enterprise RDBMS as a datasource"){
			public void actionPerformed(ActionEvent e) {
				ExampleFrame.start("Enterprise RDBMS as a datasource", "rdbms-as-a-datasource", "h2-vdb.xml", "org.teiid.example.basic.TeiidEmbeddedH2DataSource");
			}}));
		
		contentPane.add(new JButton(new AbstractAction("Microsoft Excel as a datasource"){
			public void actionPerformed(ActionEvent e) {
				ExampleFrame.start("Office Productivity Microsoft Excel as a datasource", "excel-as-a-datasource", "excel-vdb.xml", "org.teiid.example.basic.TeiidEmbeddedExcelDataSource");
			}}));
		
		contentPane.add(new JButton(new AbstractAction("Generic SOAP"){
			public void actionPerformed(ActionEvent e) {
				ExampleFrame.start("Consume a Generic SOAP Service", "generic-soap", "webservice-vdb.xml", "org.teiid.example.basic.GenericSoap");
			}}));
		
		contentPane.add(new JButton(new AbstractAction("Rest Web Service as a datasource"){
			public void actionPerformed(ActionEvent e) {
				ExampleFrame.start("Rest Web Service as a datasource", "webservices-as-a-datasource", "restwebservice-vdb.xml", "org.teiid.example.basic.TeiidEmbeddedRestWebServiceDataSource");
			}}));
		
		contentPane.add(new JButton(new AbstractAction("MongoDB as a datasource"){
			public void actionPerformed(ActionEvent e) {
				ExampleFrame.start("NoSQL MongoDB as a datasource", "mongodb-as-a-datasource", "mongodb-vdb.xml", "org.teiid.example.basic.TeiidEmbeddedMongoDBDataSource");
			}}));
		
		contentPane.add(new JButton(new AbstractAction("Cassandra as a datasource"){
			public void actionPerformed(ActionEvent e) {
				ExampleFrame.start("NoSQL Cassandra as a datasource", "cassandra-as-a-datasourse", "cassandra-vdb.xml", "org.teiid.example.basic.TeiidEmbeddedCassandraDataSource");
			}}));
		
		contentPane.add(new JButton(new AbstractAction("LDAP as a datasource"){
			public void actionPerformed(ActionEvent e) {
				ExampleFrame.start("LDAP as a datasource", "ldap-as-a-datasource", "ldap-vdb.xml", "org.teiid.example.basic.TeiidEmbeddedLDAPDataSource");
			}}));
		
		contentPane.add(new JButton(new AbstractAction("HBase as a datasource"){
			public void actionPerformed(ActionEvent e) {
				ExampleFrame.start("HBase as a datasource", "hbase-as-a-datasource", "hbase-vdb.xml", "org.teiid.example.basic.TeiidEmbeddedCassandraDataSource");
			}}));
		
		contentPane.add(new JLabel("Data Federation Examples"));
		
		contentPane.add(new JButton(new AbstractAction("Embedded Portfolio"){
			public void actionPerformed(ActionEvent e) {
				ExampleFrame.start("Embedded Portfolio", "embedded-portfolio", "portfolio-vdb.xml", "org.teiid.example.federation.TeiidEmbeddedPortfolio");
			}}));
		
		contentPane.add(new JLabel("Data Roles Examples"));
		
		contentPane.add(new JButton(new AbstractAction("Embedded dataroles"){
			public void actionPerformed(ActionEvent e) {
				ExampleFrame.start("Embedded dataroleso", "dataroles", "portfolio-vdb.xml", "org.teiid.example.dataroles.TeiidEmbeddedDataRole");
			}}));
		
		return contentPane;
	}

	public static void main(String[] args) {

		TeiidExamplesApp teiidExamplesApp = new TeiidExamplesApp();
		teiidExamplesApp.pack();
		teiidExamplesApp.setVisible(true);
	}

}
