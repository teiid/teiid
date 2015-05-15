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

import java.awt.BorderLayout;
import java.awt.EventQueue;
import java.awt.Font;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.lang.reflect.Method;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import static org.teiid.example.util.FileUtils.readFileContent;

public class ExampleFrame extends JFrame {

	private static final long serialVersionUID = 8588012832254988402L;	
	
	final JTextArea area = new JTextArea(20,150);
	
	private boolean isEdit = false;

	public ExampleFrame(String name, final String pathName, final String vdbName, final String className) {
	
		super(name);
        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        
        area.setFont(new Font("Monospaced", Font.PLAIN, 12));
		JScrollPane scroll = new JScrollPane(area,JScrollPane.VERTICAL_SCROLLBAR_ALWAYS,JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
		add(scroll,BorderLayout.CENTER);
		area.setText(readFileContent(pathName, "readme.txt"));
		
		JMenuBar bar = new JMenuBar();
		setJMenuBar(bar);
		JMenu readme = new JMenu("ReadMe");
		JMenu vdb = new JMenu("VDB");
		JMenu run = new JMenu("Run");
		
		bar.add(readme);
		bar.add(vdb);
		bar.add(run);
		
		readme.addMouseListener(new MouseAdapter(){
			@Override
			public void mouseClicked(MouseEvent e) {
				area.setText(readFileContent(pathName, "readme.txt"));
				isEdit = false;
			}
		});
		
		vdb.addMouseListener(new MouseAdapter(){
			@Override
			public void mouseClicked(MouseEvent e) {
				area.setText(readFileContent(pathName, vdbName));
				isEdit = true;
			}
		});

		run.addMouseListener(new MouseAdapter(){
			@Override
			public void mouseClicked(MouseEvent e) {
				execute(isEdit ? area.getText() : readFileContent(pathName, vdbName), className);
				isEdit = false;
			}
		});
		
		JPanel lnfPanel = new JPanel();
		JButton readmeButton = new JButton("ReadMe");
		readmeButton.addMouseListener(new MouseAdapter(){
			@Override
			public void mouseClicked(MouseEvent e) {
				area.setText(readFileContent(pathName, "readme.txt"));
				isEdit = false;
			}
		});
	    lnfPanel.add(readmeButton);
	    
	    JButton vdbButton = new JButton("VDB");
	    vdbButton.addMouseListener(new MouseAdapter(){
			@Override
			public void mouseClicked(MouseEvent e) {
				area.setText(readFileContent(pathName, vdbName));
				isEdit = true;
			}
		});
	    lnfPanel.add(vdbButton);
	    
	    JButton runButton = new JButton("Run");
	    runButton.addMouseListener(new MouseAdapter(){
			@Override
			public void mouseClicked(MouseEvent e) {
				execute(isEdit ? area.getText() : readFileContent(pathName, vdbName), className);
				isEdit = false;
			}
		});
	    lnfPanel.add(runButton);

	    add(lnfPanel, BorderLayout.SOUTH);
		
		pack();
	}
	
	private void execute(String vdb, String className) {

		try {
			Class<?> clas = this.getClass().getClassLoader().loadClass(className);
			Method method = clas.getMethod("execute", new Class[]{String.class});
			method.invoke(clas.newInstance(), new Object[]{vdb});
			area.setText("Success");
		} catch (Throwable e) {
			area.setText(e.getMessage());
			e.printStackTrace();
		}
	}


	public static void start(final String name, final String pathName, final String vdbName, final String className) {
		EventQueue.invokeLater(new Runnable() {

			public void run() {
				ExampleFrame frame = new ExampleFrame(name, pathName, vdbName, className);
				frame.setVisible(true);
			}
			
		});
	}

}
