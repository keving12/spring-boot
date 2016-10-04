package com.kg.elastic;

import com.carrotsearch.hppc.ObjectLookupContainer;
import com.kg.config.Configuration;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortParseElement;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by kevingracie on 22/09/2016.
 */
public class ScrollClient {

    private final Client client;
    private final Configuration config;


    public ScrollClient(final Configuration config) {
        this.config = config;
        client = ElasticClient.getClient(config);
    }


    public void moveDocumentMatchingQuery(final QueryBuilder builder) {
        SearchResponse searchResponse = client.prepareSearch(config.getSourceIndex())
                .addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
                .setQuery(builder)
                .setScroll(new TimeValue(config.getSearchContextTimeValue()))
                .setSize(config.getScrollSize()).execute().actionGet();
        executeDocumentMove(searchResponse);
    }

    public void moveAllDocuments() {
        SearchResponse searchResponse = client.prepareSearch(config.getSourceIndex())
                .addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(config.getSearchContextTimeValue()))
                .setSize(config.getScrollSize()).execute().actionGet();
        executeDocumentMove(searchResponse);
    }

    public void deleteDocumentsOfType(final String type) {
        SearchResponse searchResponse = client.prepareSearch(config.getSourceIndex())
                .setTypes(type)
                .addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(config.getSearchContextTimeValue()))
                .setSize(config.getScrollSize()).execute().actionGet();
        executeDocumentDeletion(searchResponse);
    }

    private void executeDocumentDeletion(SearchResponse searchResponse) {
        while(true) {
            for(SearchHit hit : searchResponse.getHits()) {
                DeleteResponse deleteResponse = client.prepareDelete(config.getSourceIndex(), "data", hit.getId()).get();
                if(deleteResponse.isFound()) {
                    System.out.println("Found");
                }
            }
            searchResponse = client.prepareSearchScroll(searchResponse.getScrollId()).setScroll(
                    new TimeValue(config.getSearchContextTimeValue())).execute().actionGet();
            if(searchResponse.getHits().getHits().length == 0) {
                break;
            }

        }
    }

    public void getMetadata() {
        ClusterStateResponse clusterState = client.admin().cluster().prepareState().execute().actionGet();
        ImmutableOpenMap<String, MappingMetaData> mappings = clusterState.getState().metaData().index("car_driver_v1").getMappings();
        ObjectLookupContainer<String> keys = mappings.keys();
        Iterator itr = keys.iterator();
        while(itr.hasNext()) {
            System.out.println("Next : "+itr.next());
        }


    }


    private void executeDocumentMove(SearchResponse searchResponse) {
        while(true) {
            for(SearchHit hit : searchResponse.getHits()) {
                IndexResponse indexResponse = client.prepareIndex(config.getDestinationIndex(), config.getDestinationType(), hit.getId())
                        .setSource(hit.getSource()).get();
            }
            searchResponse = client.prepareSearchScroll(searchResponse.getScrollId()).setScroll(
                    new TimeValue(config.getSearchContextTimeValue())).execute().actionGet();
            if(searchResponse.getHits().getHits().length == 0) {
                break;
            }
        }
    }
}
