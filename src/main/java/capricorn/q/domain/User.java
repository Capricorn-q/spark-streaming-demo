package capricorn.q.domain;


import java.io.Serializable;

/**
 * @Description : demo domain
 * @Author : Capricorn.QBB
 * @Date : 2019-04-04
 * @Version : 1.0
 */
public class User implements Serializable {
	private Integer id;
	private String name;
	private String address;
	private Integer age;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	@Override
	public String toString() {
		return "User{" +
				"id=" + id +
				", name='" + name + '\'' +
				", address='" + address + '\'' +
				", age=" + age +
				'}';
	}
}
