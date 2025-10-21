package com.example.triggerStreamTest.service;

import com.example.triggerStreamTest.model.SourceEvent;

public interface EventService {
    void routing(SourceEvent event);
}
