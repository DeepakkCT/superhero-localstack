package com.example.superhero_localstack.repository;


import com.example.superhero_localstack.model.Superhero;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface SuperheroRepository extends MongoRepository<Superhero, String> {
}