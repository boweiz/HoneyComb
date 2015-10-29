package InsertSelectByID;

import utils.connectionPool.ConnectionPool;
import utils.connectionPool.CassandraConnection;
import utils.connectionPool.DBConnection;
import java.sql.PreparedStatement;

import com.datastax.driver.core.PreparedStatement;

public class HoneyCombReportDAO {
	
	static ConnectionPool pool = ConnectionPool.getInstance();
	static HoneyCombReportDAO instance = null;
	
	public static HoneyCombReportDAO getNewInstance(String dbName) {
		return new HoneyCombReportDAO(dbName);
	}
	
	private final String dbName;
	
	private HoneyCombReportDAO(String dbName) {
		this.dbName = dbName;
	}
	
	
	public void insert(StudentReport obj) {
		String sql = String.format("INSERT INTO %s.student_report(ID, Student_name, Sex, course1ID, course2ID, course3ID, course4ID) VALUES (?,?,?,?,?,?,?,?)", dbName);

		CassandraConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(sql);
			_constructPS(ps, obj, 0);
			ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps);
		}
	}
	
	
	
	public StudentReport selectById(Integer id) {
		if (null == id) {
			return null;
		}
		String sql = String.format("SELECT * FROM %s.student_report WHERE ID = ?", dbName);
		StudentReport result = null;
		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			ps = conn.prepareStatement(sql);
			ps.setInt(1, ID);
			rs = ps.executeQuery();
			if (rs.next()) {
				result = _constructResult(rs);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps, rs);
		}
		return result;
	}

	public static void main(String[] args) {
		

	}

}
