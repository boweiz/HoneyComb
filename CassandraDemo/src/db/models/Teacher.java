package db.models;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Teacher {
	Long id;
	String name;
	String title;
	List<Long> courses;
	Set<String> email;
	Map<String, String> todo;
	
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

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public List<Long> getCourses() {
		return courses;
	}

	public void setCourses(List<Long> courses) {
		this.courses = courses;
	}
	
	public void setEmail(Set<String> email2) {
		
		this.email = email2;	
				
	}
	public Set<String> getEmail() {
		
		return email;	
		
		
	}
    public void setTodo(Map<String, String> todo) {
		
    	this.todo = todo;
		
	}
    
	
	
	public Map<String, String> getTodo() {
		
		return todo;
	}
	


}