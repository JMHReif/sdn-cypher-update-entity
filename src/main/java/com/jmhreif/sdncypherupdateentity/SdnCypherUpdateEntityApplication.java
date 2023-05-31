package com.jmhreif.sdncypherupdateentity;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.neo4j.cypherdsl.core.renderer.Configuration;
import org.neo4j.cypherdsl.core.renderer.Dialect;
import org.neo4j.driver.Driver;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.neo4j.core.ReactiveDatabaseSelectionProvider;
import org.springframework.data.neo4j.core.schema.GeneratedValue;
import org.springframework.data.neo4j.core.schema.Id;
import org.springframework.data.neo4j.core.schema.Node;
import org.springframework.data.neo4j.core.schema.Relationship;
import org.springframework.data.neo4j.core.support.UUIDStringGenerator;
import org.springframework.data.neo4j.core.transaction.ReactiveNeo4jTransactionManager;
import org.springframework.data.neo4j.repository.query.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SpringBootApplication
public class SdnCypherUpdateEntityApplication {

	public static void main(String[] args) {
		SpringApplication.run(SdnCypherUpdateEntityApplication.class, args);
	}

	@Bean
	public ReactiveTransactionManager reactiveTransactionManager(Driver driver, ReactiveDatabaseSelectionProvider databaseNameProvider) {
		return new ReactiveNeo4jTransactionManager(driver, databaseNameProvider);
	}

	//Temporary addition until fix is released for elementId issue: https://github.com/spring-projects/spring-data-neo4j/issues/2728
	@Bean
	public Configuration cypherDslConfiguration() {
		return Configuration.newConfig()
				.withDialect(Dialect.NEO4J_5)
				.build();
	}

}

@RestController
@RequestMapping("/neo")
@AllArgsConstructor
class ReviewController {
	private final ReviewRepository reviewRepo;
	private final BookRepository bookRepo;

	@GetMapping
	String liveCheck() { return "Application is up"; }

	@GetMapping("/reviews")
	Flux<Review> getReviews() { return reviewRepo.findFirst1000By(); }

	@Transactional
	@PutMapping("/save")
	Mono<Review> save(@RequestBody Review review) {
		System.out.println(review);

		//set updated values for ratings
		Book dbBook = bookRepo.findBookByBook_id(review.getBook().getBook_id()).block();
		if (dbBook.getRatings_count() == null) {
			dbBook.setRatings_count(0);
		}
		review.getBook().setRatings_count(dbBook.getRatings_count()+1);
		if (dbBook.getAverage_rating() == null) {
			dbBook.setAverage_rating(0);
		}
		review.getBook().setAverage_rating(((dbBook.getAverage_rating()*dbBook.getRatings_count())+review.getRating()) / (dbBook.getRatings_count()+1));
		System.out.println(review);

		Mono<Review> savedReview = reviewRepo.save(review);
		System.out.println(savedReview.block());

		return savedReview;
	}
}

interface ReviewRepository extends ReactiveCrudRepository<Review, String> {
	Flux<Review> findFirst1000By();
}

interface BookRepository extends ReactiveCrudRepository<Book, String> {
	@Query("MATCH (b:Book {book_id: $book_id}) RETURN b;")
	Mono<Book> findBookByBook_id(String book_id);
}

@Data
@Node
class Review {
	@Id
	@GeneratedValue(UUIDStringGenerator.class)
	private String review_id;

	private Integer rating;

	@Relationship("WRITTEN_FOR")
	private Book book;
}

@Data
@Node
class Book {
	@Id
	private String book_id;

	private Integer ratings_count, average_rating;
}