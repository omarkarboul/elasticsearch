package fr.gopartner.elasticsearch.domain;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Repository
public class ProductRepository {

    private final ElasticsearchClient elasticsearchClient;

    private final String indexName = "products";

    public ProductRepository(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }


    public String createOrUpdateDocument(Product product) throws IOException {

        IndexResponse response = elasticsearchClient.index(i -> i
                .index(indexName)
                .id(product.getId())
                .document(product)
        );
        if (response.result().name().equals("Created")) {
            return "Document has been successfully created.";
        } else if (response.result().name().equals("Updated")) {
            return "Document has been successfully updated.";
        }
        return "Error while performing the operation.";
    }

    public Product getDocumentById(String productId) throws IOException {
        Product product = null;
        GetResponse<Product> response = elasticsearchClient.get(g -> g
                        .index(indexName)
                        .id(productId),
                Product.class
        );

        if (response.found()) {
            product = response.source();
            System.out.println("Product name " + product.getName());
        } else {
            System.out.println("Product not found");
        }

        return product;
    }

    public String deleteDocumentById(String productId) throws IOException {

        DeleteRequest request = DeleteRequest.of(d -> d.index(indexName).id(productId));

        DeleteResponse deleteResponse = elasticsearchClient.delete(request);
        if (Objects.nonNull(deleteResponse.result()) && !deleteResponse.result().name().equals("NotFound")) {
            return "Product with id " + deleteResponse.id() + " has been deleted.";
        }
        System.out.println("Product not found");
        return "Product with id " + deleteResponse.id() + " does not exist.";

    }

    public List<Product> searchAllDocuments() throws IOException {
        SearchRequest searchRequest = SearchRequest.of(s -> s.index(indexName));
        SearchResponse searchResponse = elasticsearchClient.search(searchRequest, Product.class);
        List<Hit> hits = searchResponse.hits().hits();
        List<Product> products = new ArrayList<>();
        for (Hit object : hits) {
            products.add((Product) object.source());
        }
        return products;
    }

    public List<Product> searchProducts(String name, String description, Double price) throws IOException {
        Query byName = MatchQuery.of(m -> m
                .field("name")
                .query(name)
        )._toQuery();

        Query byDescription = MatchQuery.of(m -> m
                .field("description")
                .query(description)
        )._toQuery();

        Query byMaxPrice = RangeQuery.of(r -> r
                .field("price")
                .gte(JsonData.of(price))
        )._toQuery();

//        SearchResponse<Product> response = elasticsearchClient.search(s -> s
//                                    .index("products")
//                                    .query(q -> q
//                                            .bool(b -> b
//                                                    .must(byName)
//                                                    .must(byDescription)
//                                                    .must(byMaxPrice)
//                                            )
//                                    ),
//                            Product.class
//                    );

        SearchResponse<Product> response = elasticsearchClient.search(s -> s
                .index("products")
                .query(q -> q
                        .multiMatch(m -> m
                                .query(String.valueOf(MatchQuery.of(k -> k
                                        .field("name")
                                        .query(name)
                                )))
                                .queryName(String.valueOf(MatchQuery.of(k -> k
                                        .field("description")
                                        .query(description)
                                )))
                                .queryName(String.valueOf(RangeQuery.of(r -> r
                                        .field("price")
                                        .gte(JsonData.of(price))
                                ))))),
                Product.class
        );

        List<Product> products = new ArrayList<>();
        List<Hit<Product>> hits = response.hits().hits();
        for (Hit<Product> hit : hits) {
            Product product = hit.source();
            products.add(product);
        }
        return products;
    }
}
