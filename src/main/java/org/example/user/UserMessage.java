package org.example.user;

public class UserMessage {
    private int id;
    private String name;
    private String LastName;
    private String message;
    // Конструктор
    public UserMessage(int id, String name, String lastName, String message) {
        this.id = id;
        this.name = name;
        LastName = lastName;
        this.message = message;
    }

    public UserMessage() {
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



    // Геттеры и сеттеры
    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getLastName() {
        return LastName;
    }


}
