package com.example.triggerStreamTest.service.impl;

import com.example.triggerStreamTest.mapper.EventMapper;
import com.example.triggerStreamTest.model.SourceEvent;
import com.example.triggerStreamTest.service.EventService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import org.springframework.web.reactive.function.client.WebClient;

@Service
@RequiredArgsConstructor
@Slf4j
public class EventServiceImpl implements EventService {

    private final WebClient.Builder webClientBuilder;

    @Override
    public void routing(SourceEvent event) {
        String service = EventMapper.findEventCategory(event.getCategory().toString());
        log.info("Categoria ricevuta: {}", service);

        webClientBuilder.build()
                .get()
                .uri("http://localhost:8081/api/v1/"+service)
                .retrieve()
                .toBodilessEntity()
                .subscribe(
                        response -> log.info("Richiesta andata a buon fine"),
                        error -> log.error("Errore: {}", error.getMessage())
                );
    }
}
