package fr.gopartner.elasticsearch.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Repository
@Slf4j
public class ProductRepository {

    private final RestHighLevelClient restHighLevelClient;

    @Value("${elasticsearch.indexName}")
    String indexName;

    private final String NAME_FIELD = "name";
    private final String DESCRIPTION_FIELD = "description";

    public ProductRepository(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    public String createOrUpdateDocument(Product product) throws IOException {
        IndexRequest indexRequest = new IndexRequest(indexName)
                .id(product.getId())
                .source(new ObjectMapper().writeValueAsString(product), XContentType.JSON);

        try {
            restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error(e.getMessage());
        }

        return "Successfully created a product";
    }

    public Product getDocumentById(String productId) throws IOException {
        GetRequest getRequest = new GetRequest(indexName, productId);

        try {
            GetResponse response = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
            if (response.isExists()) {
                String sourceAsString = response.getSourceAsString();
                Product product = new ObjectMapper().readValue(sourceAsString, Product.class);
                log.info("product name " + product.getName());
                return product;
            } else {
                log.info("product not found");
            }
        } catch (ElasticsearchException | IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public String deleteDocumentById(String productId) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(indexName, productId);

        try {
            DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);

            if (deleteResponse.getResult() == DocWriteResponse.Result.DELETED) {
                return "Product with id " + productId + " has been deleted.";
            } else if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                System.out.println("User not found");
                return "Product with id " + productId + " does not exist.";
            }
        } catch (ElasticsearchException | IOException e) {
            e.printStackTrace();
        }

        return "Error while deleting the product.";
    }

    public List<Product> searchAllDocuments() throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        try {
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            return getProducts(searchRequest, searchSourceBuilder);
        } catch (ElasticsearchException | IOException e) {
            e.printStackTrace();
        }

        return Collections.emptyList();
    }

    public List<Product> searchByKeyword(String keyWord) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        try {
            MultiMatchQueryBuilder multiMatchQuery = QueryBuilders.multiMatchQuery(keyWord)
                    .operator(Operator.OR)
                    .field(NAME_FIELD)
                    .field(DESCRIPTION_FIELD)
                    .fuzziness(Fuzziness.AUTO);

            searchSourceBuilder.query(multiMatchQuery);
            return getProducts(searchRequest, searchSourceBuilder);
        } catch (ElasticsearchException | IOException e) {
            e.printStackTrace();
        }

        return Collections.emptyList();
    }

    private List<Product> getProducts(SearchRequest searchRequest, SearchSourceBuilder searchSourceBuilder) throws IOException {
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        List<Product> products = new ArrayList<>();

        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            Product product = new ObjectMapper().readValue(sourceAsString, Product.class);
            products.add(product);
        }

        return products;
    }
}
