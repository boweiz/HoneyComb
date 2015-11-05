/* when use update, the element in "where" should be labeled as primary key
 * however, if there are multiple primary key, every time do the select work, all primary key should be defined.
 * 
 */


package db.daos;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import utils.connectionPool.CassandraConnection;
import utils.connectionPool.ConnectionPool;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import db.models.Teacher;

public class TeacherDAO {
	static ConnectionPool pool = ConnectionPool.getInstance();
	static TeacherDAO instance = null;

	public static TeacherDAO getInstance() {
		if (null == instance) {
			instance = new TeacherDAO("demo");
		}
		return instance;
	}

	public static TeacherDAO getNewInstance(String keyspaceName) {
		return new TeacherDAO(keyspaceName);
	}
	
	private final String keyspaceName;

	private TeacherDAO(String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}

	private void createTable() {
		String sql = String
				.format("CREATE TABLE %s.teacher (id bigint primary key, name text, title text,courses list<bigint>, email Set<text>)",
						keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		try {
			conn.execute(sql);
		} finally {
			conn.close();
		}
	}

	private void dropTable() {
		String sql = String.format("DROP TABLE %s.teacher", keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		try {
			conn.execute(sql);
		} finally {
			conn.close();
		}
	}

	private void insert(Teacher obj) {
		String sql = String
				.format("INSERT INTO %s.teacher(id,name,title,courses,email) VALUES (?,?,?,?,?)",
						keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		try {
			PreparedStatement ps = conn.prepare(sql);
			System.out.println(ps);
			BoundStatement bs = ps.bind(obj.getId(), obj.getName(), obj.getTitle(), obj.getCourses(), obj.getEmail());
			conn.execute(bs);
		} finally {
			conn.close();
		}
	}
	
	// TODO
	private void updateById(List<Long> course, Set<String> email, Long id) {
		String sql = String
				.format("UPDATE %s.teacher SET courses = ?, email = ? WHERE id = ?",
						keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		try {
			PreparedStatement ps = conn.prepare(sql);
			System.out.println(ps);
			BoundStatement bs = ps.bind(course, email, id);
			conn.execute(bs);
		} finally {
			conn.close();
		}
	}
	
	// TODO
	private void addToSet(String email, Long id) {
		
		String sql = String
				.format("UPDATE %s.teacher SET email = email + { '"+ email+ "'  }  WHERE id = ?",
						keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		try {
			PreparedStatement ps = conn.prepare(sql);
			System.out.println(ps);
			BoundStatement bs = ps.bind(id);
			conn.execute(bs);
		} finally {
			conn.close();
		}	
		
	}
	
	// TODO
	private void deleteFromSet(String email, Long id) {
			
		String sql = String
				.format("UPDATE %s.teacher SET email = email - { '"+ email+ "'  }  WHERE id = ?",
						keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		try {
			PreparedStatement ps = conn.prepare(sql);
			System.out.println(ps);
			BoundStatement bs = ps.bind(id);
			conn.execute(bs);
		} finally {
			conn.close();
		}	
		
	}
	
	// TODO
	// Used for when select ... where element = ... (element should be either primary key or index)
	private void createIndex(String index) {
	
		String sql = String
				.format("CREATE INDEX ON %s.teacher (" + index + ")", keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		try {
			conn.execute(sql);
		} finally {
			conn.close();
		}
		
	}
	
	// TODO
	private void dropIndex(String index) {
		
		String sql = String
				.format("DROP INDEX ON %s.teacher (" + index + ")", keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		try {
			conn.execute(sql);
		} finally {
			conn.close();
		}
		
	}
	
	// update title by course
	// TODO
	private void updateByContain(String title, String index, String value) {
		
		String sql = String
				.format("UPDATE %s.teacher SET title = " + title + " WHERE " + index + " CONTAINS " + "'" + value + "'",
						keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		try {
			//PreparedStatement ps = conn.prepare(sql);
			//System.out.println(ps);
			//BoundStatement bs = ps.bind(title);
			conn.execute(sql);
		} finally {
			conn.close();
		}
	}
		
	// TODO
	private List<Teacher> selectByContain(String index, String value) {
		if (null == value) {
			return null;
		}
		String sql = String.format("SELECT * FROM %s.teacher WHERE " + index + " CONTAINS " + "'" + value + "'", keyspaceName);
		System.out.println(sql);
		
		CassandraConnection conn = pool.getConnection();
		List<Teacher> result = new ArrayList<Teacher>();
		
		try {
			
			ResultSet rs = conn.execute(sql);
			
			for (Row row : rs) {
				Teacher obj = new Teacher();
				setInfor(obj, row);
				result.add(obj);			
			}
		} finally {
			conn.close();
		}
		return result;
	}
	

	private void setInfor(Teacher obj, Row row) {
		
		obj.setId(row.getLong("id"));
	    obj.setName(row.getString("name"));
	    obj.setTitle(row.getString("title"));
	    obj.setCourses(row.getList("courses", Long.class));
	    obj.setEmail(row.getSet("email", String.class));

	}
	
	

	private List<Teacher> selectById(Long id) {
		if (null == id) {
			return null;
		}
		String sql = String.format("SELECT * FROM %s.teacher WHERE id = ?", keyspaceName);
		System.out.println(sql);
		
		CassandraConnection conn = pool.getConnection();
		List<Teacher> result = new ArrayList<Teacher>();
		
		try {
			PreparedStatement ps = conn.prepare(sql);
			BoundStatement bs = ps.bind(id);
			ResultSet rs = conn.execute(bs);
			
			for (Row row : rs) {
				Teacher obj = new Teacher();
				setInfor(obj, row);
				result.add(obj);			
			}
		} finally {
			conn.close();
		}
		return result;
	}
	
	private List<Teacher> selectTable(int id) {
		
		String sql = String.format("SELECT * FROM %s.teacher WHERE id = ?", keyspaceName);
		System.out.println(sql);
		
		CassandraConnection conn = pool.getConnection();
		List<Teacher> result = new ArrayList<Teacher>();
		
		try {
			PreparedStatement ps = conn.prepare(sql);
			BoundStatement bs = ps.bind(id);
			ResultSet rs = conn.execute(bs);
			
			for (Row row : rs) {
				Teacher obj = new Teacher();
				setInfor(obj, row);
				result.add(obj);			
			}
		} finally {
			conn.close();
		}
		return result;
	}
	
	private List<Teacher> selectAll() {
		String sql = String.format("SELECT * FROM %s.teacher", keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		List<Teacher> result = new ArrayList<Teacher>();
		try {
			ResultSet rs = conn.execute(sql);
			for (Row row : rs) {    
				Teacher obj = new Teacher();
				obj.setId(row.getLong("id"));
				obj.setName(row.getString("name"));
				obj.setTitle(row.getString("title"));
				obj.setCourses(row.getList("courses", Long.class));
				obj.setEmail(row.getSet("email", String.class));
				result.add(obj);
			}
		} finally {
			conn.close();
		}
		return result;
	}
	
	public static void main(String[] args) {
		TeacherDAO teacherDao = TeacherDAO.getInstance();
		 teacherDao.dropTable();
		 teacherDao.createTable();

		 Teacher obj1 = new Teacher();
		 obj1.setName("Ravi");
		 obj1.setId(1l);
		 obj1.setTitle("Teacher");
		 List<Long> course = new ArrayList<Long>();
		 course.add(15213l);
		 course.add(10000l);
		 obj1.setCourses(course);	 
		 Set<String> email = new HashSet<String>();
		 email.add("email1");		
		 obj1.setEmail(email);
		 teacherDao.insert(obj1);
		 
		 // TEST addToSet
		 teacherDao.addToSet("email2, email3", 3l);
		 
		 
		 course.add(000l);
		 
		 // TEST updateById
		 //teacherDao.updateById(course, email, 3l);
		 
		 teacherDao.createIndex("email");
//teacherDao.createIndex("course");

// TEST updateByContain
//teacherDao.updateByContain("professor", "email", "email1");
		 
		 Teacher obj2 = new Teacher();
		 
		 obj2.setName("John");
		 obj2.setId(2l);
		 obj2.setTitle("Teacher");
		 List<Long> course2 = new ArrayList<Long>();
		 course2.add(2000l);
		 course2.add(3000l);
		 obj2.setCourses(course2);	 
		 Set<String> email2 = new HashSet<String>();
		 email2.add("email6");
		 email2.add("email9");		 
		 obj2.setEmail(email2);
		 
		 teacherDao.insert(obj2);
		 
		 // TEST deleteFromSet
		 teacherDao.deleteFromSet("email6", 4l);
		 
		
		 // TEST selectbyContain
		 List<Teacher> selectbyContain = teacherDao.selectByContain("email", "email1");
		 for (Teacher t : selectbyContain) {
			 System.out.println(t.getId() + "\t|\t" + t.getName() + "\t|\t" + t.getTitle() + "\t|\t" + t.getCourses() + "\t|\t" + t.getEmail());
		 }
			
		/*
		 List<Teacher> selectbyid = teacherDao.selectById(3l);
		 for (Teacher t : selectbyid) {
		 		System.out.println(t.getId() + "\t|\t" + t.getName() + "\t|\t" + t.getTitle() + "\t|\t" + t.getCourses() + "\t|\t" + t.getEmail());
		 }
	
		
		 List<Teacher> selectAll = teacherDao.selectAll();
		 for (Teacher t : selectAll) {
		 	System.out.println(t.getId() + "\t|\t" + t.getName() + "\t|\t" + t.getTitle() + "\t|\t" + t.getCourses() + "\t|\t" + t.getEmail());		
			
		 }
		 */
		
		
		 ConnectionPool.getInstance().close();
	}
}