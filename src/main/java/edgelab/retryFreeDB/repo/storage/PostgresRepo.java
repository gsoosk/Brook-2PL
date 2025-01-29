package edgelab.retryFreeDB.repo.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import edgelab.retryFreeDB.repo.concurrencyControl.DBTransaction;
import edgelab.retryFreeDB.repo.storage.DTO.DBData;
import edgelab.retryFreeDB.repo.storage.DTO.DBDeleteData;
import edgelab.retryFreeDB.repo.storage.DTO.DBInsertData;
import edgelab.retryFreeDB.repo.storage.DTO.DBWriteData;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.PGConnection;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

@Slf4j
public class PostgresRepo implements KeyValueRepository {
    private static final String user = "username";
    private static final String password = "password";
    private static String url = "";

    private final HikariDataSource dataSource;
    public PostgresRepo(String url) {


        HikariConfig connectionPoolConfig = new HikariConfig();
        connectionPoolConfig.setJdbcUrl(url);
        connectionPoolConfig.setUsername(user);
        connectionPoolConfig.setPassword(password);

        connectionPoolConfig.setMaximumPoolSize(200);
//        connectionPoolConfig.setMinimumIdle(5);
        connectionPoolConfig.setAutoCommit(false);
//        connectionPoolConfig.setIdleTimeout(30000);
//        connectionPoolConfig.setMaxLifetime(1800000);
//        connectionPoolConfig.setConnectionTimeout(30000);

        this.dataSource = new HikariDataSource(connectionPoolConfig);


    }



    private void setDeadlockDetectionTimeout(Connection conn, String timeout) throws SQLException {
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("SET deadlock_timeout = '" + timeout + "'");
        } catch (SQLException e) {
            log.error("Could not initialize deadlock detection");
            throw e;
        }

    }



    private void delay(long duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection connect() throws SQLException {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
//            conn.setAutoCommit(false);
//            setDeadlockDetectionTimeout(conn, "1s");
            log.info("Connection created");
        } catch (SQLException e) {
            log.info(e.getMessage());
            throw e;
        }
        return conn;
    }

    @Override
    public void insert(DBTransaction tx, DBInsertData data) throws SQLException {
        Connection conn = tx.getConnection();
        String SQL = data.getNewRecord().isEmpty() ? "INSERT INTO " + data.getTable() + " VALUES  (" + data.getRecordId() + ")"
                : "INSERT INTO " + data.getTable() + " VALUES  (" + data.getRecordId() + "," + data.getNewRecord() + ")";
        log.info("insert {}:{}", data.getTable(), data.getRecordId());
        try {
            PreparedStatement pstmt = conn.prepareStatement(SQL);
            pstmt.executeUpdate();
            delay(OPERATION_THINKING_TIME);
        } catch (SQLException e) {
            log.error("Could not insert the data: {}", e.getMessage());
            throw e;
        }
    }

    @Override
    public void remove(DBTransaction tx, DBDeleteData data) throws Exception {
        Connection conn = tx.getConnection();

        String SQL = "DELETE FROM "+ data.getTable() +" WHERE ";
        for (int i = 0 ; i < data.getIds().size(); i++) {
            if (i != 0)
                SQL += "AND ";
            SQL += data.getIds().get(i) + " = ? ";
        }

        try {
            PreparedStatement pstmt = conn.prepareStatement(SQL);
            for (int i = 1; i <= data.getQueries().size() ; i++)
                pstmt.setInt(i, data.getQueries().get(i-1));
            pstmt.executeUpdate();
            delay(OPERATION_THINKING_TIME);
        } catch (SQLException e) {
            log.error("Could not remove the data: {}", e.getMessage());
            throw e;
        }
    }

    @Override
    public void write(DBTransaction tx, DBWriteData data) throws Exception {
        Connection conn = tx.getConnection();

        String SQL = "UPDATE " + data.getTable() + " SET  " + data.getVariable() + " = " + data.getValue() + " WHERE ";
        for (int i = 0 ; i < data.getIds().size(); i++) {
            if (i != 0)
                SQL += "AND ";
            SQL += data.getIds().get(i) + " = ? ";
        }

        log.info("update {}:<{}, {}>", data.getTable(), data.getIds(), data.getQueries());
        try {
            PreparedStatement pstmt = conn.prepareStatement(SQL);
            for (int i = 1; i <= data.getQueries().size() ; i++)
                pstmt.setInt(i, data.getQueries().get(i-1));
            pstmt.executeUpdate();
            delay(OPERATION_THINKING_TIME);
        } catch (SQLException e) {
            log.error("Could not write the data: {}", e.getMessage());
            throw e;
        }
    }

    @Override
    public String read(DBTransaction tx, DBData data) throws Exception {
        Connection conn = tx.getConnection();
        StringBuilder value = new StringBuilder();
        String SQL = "SELECT * FROM "+ data.getTable() +" WHERE ";
        for (int i = 0; i < data.getIds().size(); i++) {
            if (i != 0)
                SQL += "AND ";
            SQL += data.getIds().get(i) + " = ? ";
        }
        log.info("get {}:{}", data.getTable(), data.getIds());
        try {
            PreparedStatement pstmt = conn.prepareStatement(SQL);
            for (int i = 1; i <= data.getQueries().size() ; i++)
                pstmt.setInt(i, data.getQueries().get(i-1));
            ResultSet rs = pstmt.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            if (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    Object columnValue = rs.getObject(i);
                    value.append(metaData.getColumnName(i)).append(":");
                    if (columnValue instanceof String)
                        value.append("'").append(columnValue).append("'");
                    else
                        value.append(columnValue);
                    if (i != columnCount)
                        value.append(",");
                }
            }
        } catch (SQLException ex) {
            log.error("could not read: {}", ex.getMessage());
            throw ex;
        }
        return value.toString();
    }

    @Override
    public Integer lastId(String table) throws SQLException {

        try (Connection conn = connect()) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet columns = metaData.getColumns(null, null, table.toLowerCase(), null)) {
                if (columns.next()) {
                    String firstColumnName = columns.getString("COLUMN_NAME");
                    String query = "SELECT MAX(\"" + firstColumnName + "\") FROM " + table;
                    try (Statement statement = conn.createStatement();
                         ResultSet resultSet = statement.executeQuery(query)) {
                        if (resultSet.next()) {
                            int nextId = Integer.parseInt(resultSet.getString(1));
                            conn.close();
                            return nextId;
                        }
                    }
                }
            }
        } catch (SQLException ex) {
            log.info(ex.getMessage());
            throw ex;
        }

        return 0;
    }

    @Override
    public void rollback(DBTransaction tx) throws Exception {
        Connection conn = tx.getConnection();
        ((PGConnection) conn).cancelQuery();

        conn.rollback();
        conn.close();

    }
}
