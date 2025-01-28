package com.example.superhero_localstack.service;




import com.example.superhero_localstack.model.Superhero;
import com.example.superhero_localstack.repository.SuperheroRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class SuperheroService {

    @Autowired
    private SuperheroRepository superheroRepository;

    public List<Superhero> getAllSuperheroes() {
        return superheroRepository.findAll();
    }

    public Optional<Superhero> getSuperheroById(String id) {
        return superheroRepository.findById(id);
    }

    public Superhero createSuperhero(Superhero superhero) {
        return superheroRepository.save(superhero);
    }

    public Superhero updateSuperhero(String id, Superhero superhero) {
        superhero.setId(id);
        return superheroRepository.save(superhero);
    }

    public void deleteSuperhero(String id) {
        superheroRepository.deleteById(id);
    }
}