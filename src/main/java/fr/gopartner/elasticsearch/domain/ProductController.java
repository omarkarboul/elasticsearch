package fr.gopartner.elasticsearch.domain;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;

@RestController
public class ProductController {

    private final ProductRepository elasticSearchQuery;

    public ProductController(ProductRepository elasticSearchQuery) {
        this.elasticSearchQuery = elasticSearchQuery;
    }

    @PostMapping("/document")
    public ResponseEntity<Object> createOrUpdateDocument(@RequestBody Product product) throws IOException {
        String response = elasticSearchQuery.createOrUpdateDocument(product);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/document/{productId}")
    public ResponseEntity<Object> getDocumentById(@PathVariable String productId) throws IOException {
        Product product = elasticSearchQuery.getDocumentById(productId);
        return new ResponseEntity<>(product, HttpStatus.OK);
    }

    @DeleteMapping("/document")
    public ResponseEntity<Object> deleteDocumentById(@RequestParam String productId) throws IOException {
        String response = elasticSearchQuery.deleteDocumentById(productId);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/document/all")
    public ResponseEntity<Object> searchAllDocument() throws IOException {
        List<Product> products = elasticSearchQuery.searchAllDocuments();
        return new ResponseEntity<>(products, HttpStatus.OK);
    }

    @GetMapping("/document/search")
    public ResponseEntity<Object> searchProducts(@RequestParam(name = "keyword", required = false) String keyword) throws IOException {
        List<Product> products = elasticSearchQuery.searchByKeyword(keyword);
        return new ResponseEntity<>(products, HttpStatus.OK);
    }
}
