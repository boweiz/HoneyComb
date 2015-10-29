package db.models;

import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class Student {


	Long id;
	String name;
	String sex;
	
	List<Long> courses;
	List<Integer> grades;
	
	
	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSex() {
		return sex;
	}

	public void setSex(String sex) {
		this.sex = sex;
	}

	public List<Long> getCourses() {
		return courses;
	}

	public void setCourses(List<Long> courses) {
		this.courses = courses;
	}
	
	public List<Integer> getGrades() {
		return grades;
	}

	public void setGrades(List<Integer> grades) {
		this.grades = grades;
	}
	
	

}