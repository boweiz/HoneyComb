package example.write;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraDemo2 {
	
	Cluster cluster;
	Session session;

	public CassandraDemo2() {
		cluster = Cluster.builder().addContactPoint("localhost").build();
		System.out.println(cluster.toString());
		session = cluster.connect();
	}

	
	//TODO
	public void createKeyspace(String key) { // create database
		
		session.execute("CREATE KEYSPACE IF NOT EXISTS test2 WITH replication " + 
			      "= {'class':'SimpleStrategy', 'replication_factor':3};");
		

	}

	public void useKeySpace(String key) {
		session = cluster.connect(key);
	}

	public void createTableDemo() {
		System.out.println("---Creating table----");
		session.execute("CREATE TABLE test2.songs (id int PRIMARY KEY,title text, album text,artist text,tags set<text>,data blob);");
	}

	public void alterTableDemo() {
		System.out.println("---Altering table----");
		session.execute("ALTER TABLE test2.songs DROP data");
	}

	public void dropTableDemo() {
		System.out.println("---Droping table----");
		session.execute("DROP TABLE test2.songs");
	}

	public void insertDemo() {
		System.out.println("---Inserting data----");
		session.execute("INSERT INTO test2.songs (id, title, album, artist, tags)"
				+ " VALUES (1, 'T1', 't1', 'a1', {'tag1', 'tag2'})");
		session.execute("INSERT INTO test2.songs (id, title, album, artist, tags)"
				+ " VALUES (2, 'T2', 't2', 'a2', {'tag2', 'tag3'})");
		session.execute("INSERT INTO test2.songs (id, title, album, artist, tags)"
				+ " VALUES (3, 'T3', 't4', 'a3', {'tag1', 'tag3'})");
	}

	
	
	//TODO
	public void selectBySetConstrainDemo() {
		System.out.println("---Fetching data----");
		System.out.println("aaaa");
		
		ResultSet rs = session.execute("SELECT * FROM test2.songs WHERE tags CONTAINS 'tag3'");
		
		for(Row row : rs) {
			System.out.println("aaaa");
			System.out.println(row.getString("title") + "\t" + row.getString("artist"));
		}
		
	}
	
	PreparedStatement prepareStatement = 
			session.prepare(
			"select * from mykeyspace.tablename where a=? and b=?");
			BoundStatement bindStatement = 
			     new BoundStatement(prepareStatement).bind("1","2");
			session.execute(bindStatement);
	
	
	
	
	
	
	
	
	
	
	
	
	public void selectDemo() {
		System.out.println("---Select data by tag----");
		ResultSet rs = session.execute("SELECT * FROM test2.songs");
	
		
		for(Row row : rs) {
			System.out.println(row.getString("title") + "\t" + row.getString("artist"));
		}
	}
	
	
	
	
	
	public void deleteDemo() {
		System.out.println("---Deleting data----");
		session.execute("DELETE FROM test.songs WHERE id = 2");
	}

	public void close() {
		session.close();
		cluster.close();
	}

	public static void main(String[] args) {
		String key = "test2";
		CassandraDemo2 demo = new CassandraDemo2();
		demo.createKeyspace(key);
		demo.useKeySpace(key);
		try { 
			demo.createTableDemo();
			demo.alterTableDemo();
			demo.insertDemo();
//			demo.selectDemo();
			demo.selectBySetConstrainDemo();
//			demo.deleteDemo();
//			demo.selectDemo();
		} catch (com.datastax.driver.core.exceptions.AlreadyExistsException e) {
			e.printStackTrace();
		} finally {
//			demo.dropTableDemo();
			demo.close();
		}
	}
}
