package db.daos;

import java.util.ArrayList;
import java.util.List;

import utils.connectionPool.CassandraConnection;
import utils.connectionPool.ConnectionPool;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import db.models.Student;
import db.models.Teacher;

public class StudentDAO {
	
	static ConnectionPool pool = ConnectionPool.getInstance();
	static StudentDAO instance = null;
	
	public static StudentDAO getInstance() {
		if (null == instance) {
			instance = new StudentDAO("demo");
		}
		return instance;
	}

	public static StudentDAO getNewInstance(String keyspaceName) {
		return new StudentDAO(keyspaceName);
	}

	private final String keyspaceName;

	private StudentDAO(String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}

	private void createTable() {
		String sql = String
				.format("CREATE TABLE %s.student (id bigint PRIMARY KEY,name text, sex text,courses list<bigint>, grades list<int>)",
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
		String sql = String.format("DROP TABLE %s.student", keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		try {
			conn.execute(sql);
		} finally {
			conn.close();
		}
	}

	private void insert(Student obj) {
		String sql = String
				.format("INSERT INTO %s.student(id,name,sex,courses,grades) VALUES (?,?,?,?,?)",
						keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		try {
			PreparedStatement ps = conn.prepare(sql);
			System.out.println(ps);
			BoundStatement bs = ps.bind(obj.getId(), obj.getName(), obj.getSex(), obj.getCourses(), obj.getGrades());
			conn.execute(bs);
		} finally {
			conn.close();
		}
	}
	

	public List<Student> selectById(Long id) {
		if (null == id) {
			return null;
		}
		String sql = String.format("SELECT * FROM %s.student WHERE id = ?", keyspaceName);
		System.out.println(sql);
		
		CassandraConnection conn = pool.getConnection();
		List<Student> result = new ArrayList<Student>();
		
		try {
			PreparedStatement ps = conn.prepare(sql);
			BoundStatement bs = ps.bind(id);
			ResultSet rs = conn.execute(bs);
			
			for (Row row : rs) {
				Student obj = new Student();
				setInfor(obj, row);
				result.add(obj);			
			}
		} finally {
			conn.close();
		}
		return result;
	}

	
	private void setInfor(Student obj, Row row) {
		
		obj.setId(row.getLong("id"));
	    obj.setName(row.getString("name"));
	    obj.setSex(row.getString("sex"));
	    obj.setCourses(row.getList("courses", Long.class));
	    obj.setGrades(row.getList("grades", Integer.class));

	}
	
	private List<Student> selectAll() {
		String sql = String.format("SELECT * FROM %s.student", keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		List<Student> result = new ArrayList<Student>();
		
		try {
			ResultSet rs = conn.execute(sql);
			
			for (Row row : rs) {
				Student obj = new Student();
				setInfor(obj, row);
				result.add(obj);			
			}
		
		} finally {
			conn.close();
		}
		return result;
	}
	
	
	
	
	

	public static void main(String[] args) {
		StudentDAO studentDao = StudentDAO.getInstance();
		 studentDao.dropTable();
		 studentDao.createTable();

		 Student obj1 = new Student();	 
		 obj1.setId((long)10111);
		 obj1.setName("xiaohong");
		 obj1.setSex("Female");		 
		 List<Long> course = new ArrayList<Long>();
		 course.add((long) 11701);
		 course.add((long) 15213);
		 obj1.setCourses(course);		 
		 List<Integer> grades = new ArrayList<Integer>();
		 grades.add(90);
		 grades.add(100);
		 obj1.setGrades(grades);
		 studentDao.insert(obj1);
		
		 Student obj2 = new Student();		 
		 obj2.setId((long)10112);
		 obj2.setName("xiaoming");
		 obj2.setSex("Male");		 
		 List<Long> course2 = new ArrayList<Long>();
		 course2.add((long) 11701);
		 course2.add((long) 15213);
		 obj2.setCourses(course2);		 
		 List<Integer> grades2 = new ArrayList<Integer>();
		 grades2.add(88);
		 grades2.add(79);
		 obj2.setGrades(grades2);
		 studentDao.insert(obj2);

		List<Student> selectbyid = studentDao.selectById((long)10112);
		for (Student t : selectbyid) {
			System.out.println(t.getId() + "\t|\t" + t.getName() + "\t|\t" + t.getSex() + "\t|\t" + t.getCourses() + "\t|\t" + t.getGrades());
		}

		
		List<Student> selectAll = studentDao.selectAll();
		for (Student t : selectAll) {
			System.out.println(t.getId() + "\t|\t" + t.getName() + "\t|\t" + t.getSex() + "\t|\t" + t.getCourses() + "\t|\t" + t.getGrades());
		}
		
		ConnectionPool.getInstance().close();
	}
}