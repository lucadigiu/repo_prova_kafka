package com.example.triggerStreamTest.mapper;


public class EventMapper {
    public static String findEventCategory(String event) {
        return switch (event) {
            case "USER_CONFIG" -> "user-service";
            case "OTHER" -> "other-service";
            default -> null;
        };
    }

}