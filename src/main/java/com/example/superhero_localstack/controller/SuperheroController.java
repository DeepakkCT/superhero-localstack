package com.example.superhero_localstack.controller;

import com.example.superhero_localstack.model.Superhero;
import com.example.superhero_localstack.service.SuperheroMessageProducer;
import com.example.superhero_localstack.service.SuperheroService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api")
public class SuperheroController {

    @Autowired
    private SuperheroService superheroService;

    @Autowired
    private SuperheroMessageProducer messageProducer;


    @PostMapping("/queue")
    public ResponseEntity<String> queueSuperhero(@RequestBody Superhero superhero) {
        messageProducer.sendMessage(superhero);
        return ResponseEntity.ok("Superhero queued for processing");
    }

    @PutMapping("/queue/{id}")
    public ResponseEntity<String> queueSuperheroUpdate(
            @PathVariable String id,
            @RequestBody Superhero superhero) {
        superhero.setId(id);  // Set the ID for update
        messageProducer.sendUpdateMessage(superhero);
        return ResponseEntity.ok("Superhero update queued for processing");
    }


    @GetMapping("/all")
    public List<Superhero> getAllSuperheroes() {
        return superheroService.getAllSuperheroes();
    }

    @GetMapping("/{id}")
    public ResponseEntity<Superhero> getSuperheroById(@PathVariable String id) {
        return superheroService.getSuperheroById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/add")
    public Superhero createSuperhero(@RequestBody Superhero superhero) {
        return superheroService.createSuperhero(superhero);
    }

    @PutMapping("/{id}")
    public ResponseEntity<Superhero> updateSuperhero(@PathVariable String id,
                                                     @RequestBody Superhero superhero) {
        return ResponseEntity.ok(superheroService.updateSuperhero(id, superhero));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteSuperhero(@PathVariable String id) {
        superheroService.deleteSuperhero(id);
        return ResponseEntity.ok().build();
    }
}
