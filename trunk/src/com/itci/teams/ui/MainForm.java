package com.itci.teams.ui;

import com.itci.teams.DAO;

import javax.swing.*;
import javax.swing.event.ListSelectionListener;
import javax.swing.event.ListSelectionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import java.awt.event.ContainerAdapter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

public class MainForm {
    private JPanel mainPanel;
    private JList schemasList;
    private JProgressBar progressBar;
    private JList tableList;
    private JTextArea taSqlloaderHeader;
    private JButton bRefresh;
    private JButton bStartStop;
    private JLabel progressLabel;
    private JTextField tfUsername;
    private JTextField tfPassword;
    private JComboBox cbTNSNAME;
    private JButton bConnect;

    private Connection conn;
    private DAO dao;
    private String schemaName;
    private String tableName;


    public MainForm() {
        bConnect.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ae) {
                if ("Connect".equals(ae.getActionCommand())) {
                    final boolean connectResult = connect();
                    if (connectResult) {
                        refreshSchemas();
                    }
                } else if ("Disconnect".equals(ae.getActionCommand())) {
                    disconnect();
                }
            }


        });
        schemasList.addListSelectionListener(new ListSelectionListener() {
            public void valueChanged(ListSelectionEvent e) {
                schemaName = (String) schemasList.getSelectedValue();
                refreshTables(schemaName);
            }
        });
        tableList.addListSelectionListener(new ListSelectionListener() {
            public void valueChanged(ListSelectionEvent e) {
                tableName = (String) tableList.getSelectedValue();
                refreshTextArea(tableName);
            }
        });
    }

    private boolean refreshTextArea(String tableName) {
        try {
            long count = dao.countRecords(schemaName, tableName);
            taSqlloaderHeader.setText("Table "+schemaName+"."+tableName+" contains " + count + " record(s).");
            return true;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean refreshTables(String schemaName) {
        try {

            final List<String> tableList = dao.getTables(schemaName);
            this.tableList.setModel(new AbstractListModel() {
                public int getSize() {
                    return tableList.size();
                }

                public Object getElementAt(int index) {
                    return tableList.get(index);
                }
            });
            return true;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean refreshSchemas() {
        try {
            final List<String> schemaList = dao.getAllSchemas();
            schemasList.setModel(new AbstractListModel() {
                public int getSize() {
                    return schemaList.size();
                }

                public Object getElementAt(int index) {
                    return schemaList.get(index);
                }
            });
            return true;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean disconnect() {
        try {
            // closing existing connection
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
            tfUsername.setEnabled(true);
            tfPassword.setEnabled(true);
            cbTNSNAME.setEnabled(true);
            bConnect.setActionCommand("Connect");
            bConnect.setText("Connect");
            System.out.println("[ OK ] Disconnected");
            schemasList.setModel(new DefaultListModel());
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("[FAIL] Disconnected");
        }
        return false;
    }

    private boolean connect() {
        try {
            // closing existing connection
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
            String connectionString = "jdbc:oracle:oci8:" + tfUsername.getText() + "/" + tfPassword.getText() + "@" + cbTNSNAME.getSelectedItem();
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
            try {
                conn = DriverManager.getConnection(connectionString);
                // success
                tfUsername.setEnabled(false);
                tfPassword.setEnabled(false);
                cbTNSNAME.setEnabled(false);
                bConnect.setActionCommand("Disconnect");
                bConnect.setText("Disconnect");
                System.out.println("[ OK ] Connected to " + connectionString);
                dao = new DAO(conn);
                return true;
            }
            catch (Exception e) {
                e.printStackTrace();
                System.out.println("[ FAIL ] Connected to " + connectionString);
            }
        }
        catch (Exception e) {
            e.printStackTrace();

        }
        return false;
    }

    public JPanel getMainPanel() {
        return mainPanel;
    }

    @Override
    protected void finalize() throws Throwable {
        disconnect();
    }
}
