package com.itci.teams;

import java.io.File;
import java.io.FileWriter;
import java.sql.*;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class Export {
    public static final DateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final String QUOTE = "\"";
    private static final String NULL = "NULL";
    private static final String COMMA = ";";
    private static final String LF = "\n";
    private static final int FLUSH_SIZE = 20;
    private static final int PAGE_SIZE = 1000;
    private static final int FETCH_SIZE = 5000;

    public static void main(String[] args) throws Exception {
        System.out.println(USAGE);
        long expStarted = System.currentTimeMillis();
        if (args.length < 2) {
            System.out.println("\nCheck usage.");
            return;
        }
        String connectionString = args[0];
        String schemaList = args[1];
        String[] schemas = schemaList.split(",");

        String[] tabs = args.length >= 3 ? args[2].split(",") : null;

        for (int schemaIndex = 0; schemaIndex < schemas.length; schemaIndex++) {
            String schema = schemas[schemaIndex];
            int exported = 0;
            int totalToExport = 0;
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
            Connection conn = DriverManager.getConnection(connectionString);
            String tablesQuery = "select owner||'.'||table_name tab_nam, table_name, owner from all_tables where owner = ?  and table_name not like '%TRACE' and table_name not like 'XXX%' and table_name not like '%HISTORY' ";
            if (tabs != null) {
                tablesQuery += " AND TABLE_NAME in (";
                for (int i = tabs.length - 1; i >= 0; i--) {
                    tablesQuery += "'" + tabs[i] + "'";
                    if (i != 0) {
                        tablesQuery += ",";
                    }
                }
                tablesQuery += ") ";
            }
            tablesQuery += " order by table_name";
            PreparedStatement ps = conn.prepareStatement(tablesQuery);
            ps.setString(1, schema);
            System.out.println(tablesQuery);
            Map tables = new TreeMap();
            {
                final ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    tables.put(resultSet.getString(1), new TableInfo(0, resultSet.getString(2), resultSet.getString(3)));
                }
                resultSet.close();
                ps.close();
            }
            System.out.println("Exporting " + schema);
            System.out.println("-------------------------------------------------------");
            {
                final Statement st = conn.createStatement();
                int totalRecords = -1;
                for (Iterator it = tables.keySet().iterator(); it.hasNext();) {
                    String tableName = (String) it.next();
                    ResultSet rs = st.executeQuery("select count(1) from " + tableName);
                    if (rs.next()) {
                        totalRecords = rs.getInt(1);
                        ((TableInfo) tables.get(tableName)).setTotalRecords(totalRecords);
                        totalToExport += totalRecords;
                    }
                    rs.close();
                    System.out.println(tableName + " : " + totalRecords);
                }
                st.close();

            }
            System.out.println("----------------------------------------------------------------");
            System.out.println("Starting export " + new java.util.Date());
            long start = System.currentTimeMillis();
            final Statement st = conn.createStatement();
            st.setFetchSize(FETCH_SIZE);
            int flushCounter = FLUSH_SIZE;
            // main loop
            for (Iterator it = tables.entrySet().iterator(); it.hasNext();) {
                int pageCounter = 0;
                final Map.Entry entry = (Map.Entry) it.next();
                final String tableName = (String) entry.getKey();
                final TableInfo tableInfo = (TableInfo) entry.getValue();
                final int tableTotalRecords = tableInfo.getTotalRecords();
                System.out.println("Exporting " + tableName + " of " + tableTotalRecords + " records");
                File dir = new File(tableInfo.getOwner());
                if (!dir.exists()) {
                    dir.mkdir();
                }
                FileWriter fw = new FileWriter(new File(dir, tableInfo.getTableName() + ".ctl"));

                fw.write("LOAD DATA\n" +
                        "INFILE *\n" +
                        "BADFILE '" + tableName + ".BAD'\n" +
                        "DISCARDFILE '" + tableName + ".DSC'\n" +
                        "TRUNCATE INTO TABLE " + tableName + "\n" +
                        "Fields terminated by \";\" Optionally enclosed by '\"'\n");

                final ResultSet rs = st.executeQuery("select * from " + entry.getKey());
                final ResultSetMetaData rsmd = rs.getMetaData();
                final int columnCount = rsmd.getColumnCount();
                fw.write("(\n");
                for (int i = 1; i <= columnCount; i++) {
                    final int columnType = rsmd.getColumnType(i);

                    String columnName = rsmd.getColumnName(i);
                    if (SQLLOADER_RESERVED.contains(columnName)) {
                        columnName = QUOTE + columnName + QUOTE;
                    }
                    fw.write("\t" + columnName);
                    if (columnType == Types.DATE || columnType == Types.TIMESTAMP
                            || columnType == Types.TIME) {
                        fw.write(" DATE \"yyyy-MM-dd HH24:mi:ss\" ");
                    }
                    if ((columnType == Types.CHAR || columnType == Types.VARCHAR || columnType == Types.LONGVARCHAR)) {
                        if (rsmd.getColumnDisplaySize(i) > 4) {
                            fw.write(" CHAR(" + rsmd.getColumnDisplaySize(i) + ")");
                        }
                    }
                    fw.write(" NULLIF (" + columnName + "=\"NULL\") ");

                    if (i != columnCount) {
                        fw.write(",");
                    }
                    fw.write("\n");
                }
                fw.write(")\n");
                fw.write("BEGINDATA\n");
                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        final int columnType = rsmd.getColumnType(i);
                        if (columnType == Types.CHAR || columnType == Types.VARCHAR || columnType == Types.LONGVARCHAR) {
                            final String value = rs.getString(i);
                            if (value != null) {
                                fw.write(QUOTE + value.replaceAll("[\r\n]+", "^").replaceAll("\"", "\"\"") + QUOTE);
                            } else {
                                fw.write(NULL);
                            }
                        } else if (columnType != Types.DATE && columnType != Types.TIMESTAMP
                                && columnType != Types.TIME) {
                            if (rs.getObject(i) == null) {
                                fw.write(NULL);
                            } else {
                                fw.write(rs.getObject(i) + "");
                            }
                        } else {
                            final Date date = rs.getDate(i);
                            if (date != null) {
                                fw.write(QUOTE + DF.format(date) + QUOTE);
                            } else {
                                fw.write(NULL);
                            }
                        }
                        if (i < columnCount) {
                            fw.write(COMMA);
                        }
                    }
                    if (flushCounter-- < 0) {
                        flushCounter = FLUSH_SIZE;
                        fw.flush();
                    }
                    exported++;
                    if (pageCounter++ == PAGE_SIZE) {

                        System.out.println("Overall progress [ " + (exported * 100 / totalToExport) + "% ] " + exported + " of " + totalToExport + "(" + ((exported) / ((System.currentTimeMillis() - start) / 1000)) + " rec/sec)");
                        pageCounter = 0;
                    }
                    fw.write(LF);
                }
                fw.close();
                rs.close();
                System.out.println("Export " + entry.getKey() + " completed - " + tableInfo.getTotalRecords() + " records");
                System.out.println("--------------------------------------------------------------");
            }
            FileWriter fw = new FileWriter(new File(schema, "load-all.bat"));
            for (Iterator it = tables.values().iterator(); it.hasNext();) {
                TableInfo tableInfo = (TableInfo) it.next();
                fw.write("sqlldr USERID=%1 CONTROL=" + tableInfo.getTableName() + ".CTL direct=true\n");
                fw.flush();
            }
            fw.close();
            long duration = (System.currentTimeMillis() - expStarted) / 1000;
            long sec = duration % 60;
            long min = (duration % 3600) / 60;
            long hrs = (duration % (3600 * 24)) / 3600;
            System.out.println("Export done in " + hrs + " hrs " + min + " min " + sec + " sec. Total records : " + totalToExport);
            System.out.println("Average speed : " + totalToExport / duration + " recodrs per sec");
        }
    }

    static class TableInfo {
        int totalRecords;
        private String tableName;
        private String owner;

        TableInfo(int totalRecords, String tableName, String owner) {
            this.totalRecords = totalRecords;
            this.tableName = tableName;
            this.owner = owner;
        }

        public int getTotalRecords() {
            return totalRecords;
        }

        public void setTotalRecords(int totalRecords) {
            this.totalRecords = totalRecords;
        }

        public String getTableName() {
            return tableName;
        }

        public String getOwner() {
            return owner;
        }
    }

    public static final Set SQLLOADER_RESERVED = new HashSet();

    {
        SQLLOADER_RESERVED.addAll(Arrays.asList(new String[]{
                "AND",
                "APPEND",
                "BADDN",
                "BADFILE",
                "BEGINDATA",
                "BFILE",
                "BLANKS",
                "BLOCKSIZE",
                "BY",
                "BYTEINT",
                "CHAR",
                "CHARACTERSET",
                "COLUMN",
                "CONCATENATE",
                "CONSTANT",
                "CONTINUE_LOAD",
                "CONTINUEIF",
                "COUNT",
                "DATA",
                "DATE",
                "DECIMAL",
                "DEFAULTIF",
                "DELETE",
                "DISABLED_CONSTRAINTS",
                "DISCARDDN",
                "DISCARDFILE",
                "DISCARDMAX",
                "DISCARDS",
                "DOUBLE",
                "ENCLOSED",
                "EOF",
                "EXCEPTIONS",
                "EXTERNAL",
                "FIELDS",
                "FILLER",
                "FIXED",
                "FLOAT",
                "FORMAT",
                "GENERATED",
                "GRAPHIC",
                "INDDN",
                "INDEXES",
                "INFILE",
                "INSERT",
                "INTEGER",
                "INTO",
                "LAST",
                "LOAD",
                "LOBFILE",
                "LOG",
                "LONG",
                "MAX",
                "MLSLABEL",
                "MONTH",
                "NESTED",
                "NEXT",
                "NO",
                "NULLCOLS",
                "NULLIF",
                "OBJECT",
                "OID",
                "OPTIONALLY",
                "OPTIONS",
                "PART",
                "PARTITION",
                "PIECED",
                "POSITION",
                "PRESERVE",
                "RAW",
                "READBUFFERS",
                "READSIZE",
                "RECLEN",
                "RECNUM",
                "RECORD",
                "RECOVERABLE",
                "REENABLE",
                "REF",
                "REPLACE",
                "RESUME",
                "SDF",
                "SEQUENCE",
                "SID",
                "SINGLEROW",
                "SKIP",
                "SMALLINT",
                "SORTDEVT",
                "SORTED",
                "SORTNUM",
                "SQL",
                "DS",
                "STORAGE",
                "STREAM",
                "SUBPARTITION",
                "SYSDATE",
                "TABLE",
                "TERMINATED",
                "THIS",
                "TRAILING",
                "TRUNCATE",
                "UNLOAD",
                "UNRECOVERABLE",
                "USING",
                "VARCHAR",
                "VARCHARC",
                "VARGRAPHIC",
                "VARIABLE",
                "VARRAW",
                "VARRAWC",
                "VARRAY",
                "WHEN",
                "WHITESPACE",
                "WORKDDN",
                "YES",
                "ZONED"}));
    }

    public static final String USAGE = "Export {full_jdbc_connection_string} {schema_list} [table_list]\n" +
            "{full_jdbc_connection_string} - smth like jdbc:oracle:oci8:scott/tiger@TMSMAIN.WORLD  (OCI) or  jdbc:oracle:oci8:scott/tiger@localhost:1521:TMSMAIN\n" +
            "{schema_list} - comma separated list of schemas to export e.g. (CORE_ENTITIES,BGM_COMPONENTS_OBJETCS) \n" +
            "[table_list]  - optional comma separated list of tables to export. If none specified - all tables will be exported.\n";
}
