package WebNoSqlStore;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sbalabanov
 */
public class StoreOci {

    private final Connection conn;
    private final PreparedStatement putStmt;
    private final PreparedStatement getStmt;

    public StoreOci() throws SQLException {
        String url = "jdbc:oracle:thin:@nisd.adm.bnet:1521:ezat";
        Properties props = new Properties();
        props.setProperty("user", "ezapori");
        props.setProperty("password", "ezapori");
        conn = DriverManager.getConnection(url, props);
        putStmt = conn.prepareStatement("insert into ARTEFACTS (art_id,art_body,art_body_short)values(?,?,'CDS')");
        getStmt = conn.prepareStatement("select art_body from ARTEFACTS where art_id=?");
    }

    public void put(String keyString, byte[] valueCont) throws SQLException  {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(valueCont);
            putStmt.setBinaryStream(2, bis);
            putStmt.setString(1, keyString);
            putStmt.execute();
        } catch (SQLException ex) {
          if (ex.getErrorCode()==1) return ;
          throw ex;
        }
    }

    String bytArrayToHex(byte[] a) {
        StringBuilder sb = new StringBuilder();
        for (byte b : a) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }

    public String put(byte[] valueCont) throws SQLException {
        byte[] dig = null;
        MessageDigest sha = null;
        try {
            sha = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException ex) {
            try {
                sha = MessageDigest.getInstance("SHA-1");
            } catch (NoSuchAlgorithmException ex1) {
                ex1.printStackTrace();
            }

        }
        long time = System.currentTimeMillis();
        String keyString = Long.toString(time);
        if (sha != null) {
            sha.update(valueCont);
            dig = sha.digest();
            keyString = bytArrayToHex(dig);
        }
        put(keyString, valueCont);
        return keyString;
    }

    public byte[] get(String keyString) {
        try {
            getStmt.setString(1, keyString);
           ResultSet rs= putStmt.executeQuery();
            while(rs.next())
            {
             return rs.getBytes(0);
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }
}
