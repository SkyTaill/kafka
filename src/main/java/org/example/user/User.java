package org.example.user;

public class User {
    private int id;
    private String name;
    private String LastName;
    private Long phoneNumber;
    // Конструктор
    public User(int id, String name, String lastName, Long phoneNumber) {
        this.id = id;
        this.name = name;
        LastName = lastName;
        this.phoneNumber = phoneNumber;
    }

    public User() {
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setLastName(String lastName) {
        LastName = lastName;
    }

    public void setPhoneNumber(Long phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    // Геттеры и сеттеры
    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }


    public String getLastName() {
        return LastName;
    }

    public Long getPhoneNumber() {
        return phoneNumber;
    }
}
