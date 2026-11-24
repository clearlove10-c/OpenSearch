/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.fieldcaps;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.index.engine.dataformat.stub.MockDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class TransportFieldCapabilitiesIndexActionTests extends OpenSearchSingleNodeTestCase {

    // The pluggable data format index needs a registered data format plus committer engine; without
    // a committer factory EngineConfigFactory rejects index creation when pluggable is enabled.
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, MockDataFormatPlugin.class, MockCommitterEnginePlugin.class);
    }

    // With disable_objects=true, {"attributes": {"foo": {"bar": "baz"}}} is flattened into the leaf
    // field "attributes.foo.bar". The intermediate path "attributes.foo" has no ObjectMapper and
    // _field_caps must not silently return empty results when walking the parent chain.
    public void testFieldCapsDisableObjects() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("attributes")
            .field("type", "object")
            .field("disable_objects", true)
            .endObject()
            .endObject()
            .endObject()
            .toString();

        assertAcked(client().admin().indices().prepareCreate("test").setMapping(mapping));

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("attributes")
                    .startObject("foo")
                    .field("bar", "baz")
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();

        client().admin().indices().prepareRefresh("test").get();

        FieldCapabilitiesResponse response = client().fieldCaps(new FieldCapabilitiesRequest().fields("*").indices("test")).actionGet();

        Map<String, Map<String, FieldCapabilities>> fields = response.get();

        assertTrue("expected attributes.foo.bar in field caps", fields.containsKey("attributes.foo.bar"));
        assertTrue("expected attributes in field caps", fields.containsKey("attributes"));
        assertEquals("object", fields.get("attributes").values().iterator().next().getType());
        // phantom intermediate path has no ObjectMapper and must not exist
        assertFalse("attributes.foo should not exist in field caps", fields.containsKey("attributes.foo"));
    }

    // Indexing a second document after a disable_objects field must not corrupt the flattened field
    // name. Previously "attributes.foo.bar" became "attributes.foo.foo.bar" after a mapping merge
    // triggered by an unrelated document.
    public void testFieldCapsDisableObjectsAfterUnrelatedDocument() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("attributes")
            .field("type", "object")
            .field("disable_objects", true)
            .endObject()
            .endObject()
            .endObject()
            .toString();

        assertAcked(client().admin().indices().prepareCreate("test").setMapping(mapping));

        // doc 1: creates the disable_objects leaf field
        client().prepareIndex("test")
            .setId("1")
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("attributes")
                    .startObject("foo")
                    .field("bar", "baz")
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();

        // doc 2: unrelated - must not corrupt the existing mapping
        client().prepareIndex("test").setId("2").setSource("{\"key\": 1}", MediaTypeRegistry.JSON).get();

        client().admin().indices().prepareRefresh("test").get();

        FieldCapabilitiesResponse response = client().fieldCaps(new FieldCapabilitiesRequest().fields("*").indices("test")).actionGet();

        Map<String, Map<String, FieldCapabilities>> fields = response.get();

        assertTrue("expected attributes.foo.bar in field caps", fields.containsKey("attributes.foo.bar"));
        assertTrue("expected attributes.foo.bar.keyword in field caps", fields.containsKey("attributes.foo.bar.keyword"));
        assertFalse("attributes.foo.foo.bar must not exist", fields.containsKey("attributes.foo.foo.bar"));
        assertTrue("expected attributes in field caps", fields.containsKey("attributes"));
        assertTrue("expected key in field caps", fields.containsKey("key"));
    }

    // Regression test: the parentPath fix in ParametrizedFieldMapper must not break normal object
    // field handling. Regular nested object fields (without disable_objects) must still produce the
    // correct parent entries in _field_caps.
    public void testFieldCapsNormalNestedObjectUnaffected() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("user")
            .field("type", "object")
            .startObject("properties")
            .startObject("address")
            .field("type", "object")
            .startObject("properties")
            .startObject("city")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();

        assertAcked(client().admin().indices().prepareCreate("test").setMapping(mapping));

        client().prepareIndex("test")
            .setId("1")
            .setSource("{\"user\":{\"address\":{\"city\":\"Chemnitz\"}}}", MediaTypeRegistry.JSON)
            .get();

        client().admin().indices().prepareRefresh("test").get();

        FieldCapabilitiesResponse response = client().fieldCaps(new FieldCapabilitiesRequest().fields("*").indices("test")).actionGet();

        Map<String, Map<String, FieldCapabilities>> fields = response.get();

        assertTrue("expected user.address.city", fields.containsKey("user.address.city"));
        assertTrue("expected user.address as object", fields.containsKey("user.address"));
        assertEquals("object", fields.get("user.address").values().iterator().next().getType());
        assertTrue("expected user as object", fields.containsKey("user"));
        assertEquals("object", fields.get("user").values().iterator().next().getType());
    }

    // Covers the pluggable branch in TransportFieldCapabilitiesIndexAction#shardOperation. A long
    // with index:false, doc_values:true has isSearchable()==false but is searchable via doc values,
    // so field caps must report searchable=true on a pluggable index and searchable=false otherwise.
    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testSearchableReportedForDocValuesOnlyFieldOnPluggableIndex() throws Exception {
        // A doc-values-only long: not indexed for search, but backed by doc values.
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("num")
            .field("type", "long")
            .field("index", false)
            .field("doc_values", true)
            .endObject()
            .endObject()
            .endObject()
            .toString();

        // Pluggable index: doc values make the field searchable, so field caps must report true.
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("pluggable")
                .setSettings(Settings.builder().put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true))
                .setMapping(mapping)
        );
        // Pluggable indices are append-only, so a custom document id is rejected; let it auto-generate.
        client().prepareIndex("pluggable").setSource("{\"num\": 42}", MediaTypeRegistry.JSON).get();
        client().admin().indices().prepareRefresh("pluggable").get();

        FieldCapabilitiesResponse pluggableResponse = client().fieldCaps(new FieldCapabilitiesRequest().fields("num").indices("pluggable"))
            .actionGet();
        assertTrue(
            "doc-values-only field must be searchable on a pluggable index",
            pluggableResponse.get().get("num").get("long").isSearchable()
        );

        // Normal index: same mapping, no pluggable setting. The field stays non-searchable.
        assertAcked(client().admin().indices().prepareCreate("normal").setMapping(mapping));
        client().prepareIndex("normal").setSource("{\"num\": 42}", MediaTypeRegistry.JSON).get();
        client().admin().indices().prepareRefresh("normal").get();

        FieldCapabilitiesResponse normalResponse = client().fieldCaps(new FieldCapabilitiesRequest().fields("num").indices("normal"))
            .actionGet();
        assertFalse(
            "doc-values-only field must not be searchable on a normal index",
            normalResponse.get().get("num").get("long").isSearchable()
        );
    }

    public void testDynamicPropertiesStayHiddenUntilRequested() throws Exception {
        createDynamicPropertiesIndex("dyn", 1, 0);

        client().prepareIndex("dyn")
            .setId("1")
            .setSource("{\"title\":\"hello\",\"count_i\":5,\"name_s\":\"alpha\"}", MediaTypeRegistry.JSON)
            .get();
        client().admin().indices().prepareRefresh("dyn").get();

        String mapping = client().admin().indices().prepareGetMappings("dyn").get().getMappings().get("dyn").source().string();
        assertFalse(mapping.contains("count_i"));
        assertFalse(mapping.contains("name_s"));

        FieldCapabilitiesResponse hidden = fieldCaps("dyn", false, "*");
        assertTrue(hidden.get().containsKey("title"));
        assertFalse(hidden.get().containsKey("count_i"));
        assertFalse(hidden.get().containsKey("name_s"));

        FieldCapabilitiesResponse visible = fieldCaps("dyn", true, "*");
        assertTrue(visible.get().containsKey("title"));
        assertLongField(visible, "count_i");
        assertKeywordField(visible, "name_s");
        Set<String> expected = new HashSet<>(hidden.get().keySet());
        expected.add("count_i");
        expected.add("name_s");
        assertEquals(expected, visible.get().keySet());
    }

    public void testIncludeDynamicPropertiesFieldPatternAndExactName() throws Exception {
        createDynamicPropertiesIndex("dyn", 1, 0);
        client().prepareIndex("dyn")
            .setId("1")
            .setSource("{\"obj\":{\"nested_i\":7},\"count_i\":1,\"name_s\":\"alpha\"}", MediaTypeRegistry.JSON)
            .get();
        client().admin().indices().prepareRefresh("dyn").get();

        FieldCapabilitiesResponse onlyLongs = fieldCaps("dyn", true, "*_i");
        assertLongField(onlyLongs, "count_i");
        assertLongField(onlyLongs, "obj.nested_i");
        assertFalse(onlyLongs.get().containsKey("name_s"));
        assertFalse(onlyLongs.get().containsKey("title"));

        // An exact name is resolved from the dynamic_properties pattern even when that name was never indexed.
        FieldCapabilitiesResponse exactWithoutFlag = fieldCaps("dyn", false, "never_seen_i");
        FieldCapabilitiesResponse exactWithFlag = fieldCaps("dyn", true, "never_seen_i");
        assertLongField(exactWithoutFlag, "never_seen_i");
        assertLongField(exactWithFlag, "never_seen_i");
        assertEquals(exactWithoutFlag.get().keySet(), exactWithFlag.get().keySet());

        FieldCapabilitiesResponse wildcardMiss = fieldCaps("dyn", true, "other*");
        assertFalse(wildcardMiss.get().containsKey("count_i"));
        assertFalse(wildcardMiss.get().containsKey("never_seen_i"));
    }

    public void testIncludeDynamicPropertiesUnionsShardsAndHidesUnrefreshedFields() throws Exception {
        createDynamicPropertiesIndex("dyn", 2, 1);
        IndexMetadata index = client().admin().cluster().prepareState().get().getState().metadata().index("dyn");
        String shard0Routing = routingForShard(index, 0);
        String shard1Routing = routingForShard(index, 1);

        client().prepareIndex("dyn").setId("doc-0").setRouting(shard0Routing).setSource("{\"count_i\":1}", MediaTypeRegistry.JSON).get();
        client().prepareIndex("dyn")
            .setId("doc-1")
            .setRouting(shard1Routing)
            .setSource("{\"name_s\":\"only-on-shard-1\"}", MediaTypeRegistry.JSON)
            .get();

        FieldCapabilitiesResponse beforeRefresh = fieldCaps("dyn", true, "*");
        assertFalse(beforeRefresh.get().containsKey("count_i"));
        assertFalse(beforeRefresh.get().containsKey("name_s"));

        client().admin().indices().prepareRefresh("dyn").get();

        FieldCapabilitiesResponse afterRefresh = fieldCaps("dyn", true, "*");
        assertLongField(afterRefresh, "count_i");
        assertKeywordField(afterRefresh, "name_s");
        assertTrue(afterRefresh.get().containsKey("title"));
    }

    public void testIncludeDynamicPropertiesKeptWhenIndexFilterMatchesNothing() throws Exception {
        createDynamicPropertiesIndex("dyn", 1, 0);
        assertAcked(
            client().admin()
                .indices()
                .preparePutMapping("dyn")
                .setSource("{\"properties\":{\"timestamp\":{\"type\":\"date\"}}}", MediaTypeRegistry.JSON)
        );
        client().prepareIndex("dyn")
            .setId("1")
            .setSource("{\"timestamp\":\"2015-07-08\",\"title\":\"hello\",\"count_i\":5}", MediaTypeRegistry.JSON)
            .get();
        client().admin().indices().prepareRefresh("dyn").get();

        FieldCapabilitiesResponse withoutFlag = client().fieldCaps(
            new FieldCapabilitiesRequest().fields("*")
                .indices("dyn")
                .indexFilter(QueryBuilders.rangeQuery("timestamp").gte("2099-01-01"))
        ).actionGet();
        assertEquals(0, withoutFlag.getIndices().length);
        assertFalse(withoutFlag.get().containsKey("count_i"));

        FieldCapabilitiesResponse withFlag = client().fieldCaps(
            new FieldCapabilitiesRequest().fields("*")
                .indices("dyn")
                .includeDynamicProperties(true)
                .indexFilter(QueryBuilders.rangeQuery("timestamp").gte("2099-01-01"))
        ).actionGet();
        assertArrayEquals(new String[] { "dyn" }, withFlag.getIndices());
        assertLongField(withFlag, "count_i");
        // Mapping caps stay tied to shards that can match the filter. This request only keeps the
        // concrete dynamic_properties fields that were read from FieldInfos.
        assertFalse(withFlag.get().containsKey("title"));
        assertFalse(withFlag.get().containsKey("timestamp"));
    }

    private void createDynamicPropertiesIndex(String index, int shards, int replicas) throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("dynamic_properties")
            .startObject("*_i")
            .field("type", "long")
            .endObject()
            .startObject("*_s")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .startObject("properties")
            .startObject("title")
            .field("type", "text")
            .endObject()
            .startObject("obj")
            .field("type", "object")
            .endObject()
            .endObject()
            .endObject()
            .toString();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicas)
                        .put("index.refresh_interval", "-1")
                )
                .setMapping(mapping)
        );
    }

    private FieldCapabilitiesResponse fieldCaps(String index, boolean includeDynamicProperties, String... fields) {
        return client().fieldCaps(
            new FieldCapabilitiesRequest().fields(fields).indices(index).includeDynamicProperties(includeDynamicProperties)
        ).actionGet();
    }

    private static String routingForShard(IndexMetadata index, int shardId) {
        for (int i = 0; i < 10_000; i++) {
            String routing = "route-" + i;
            if (OperationRouting.generateShardId(index, "doc-" + shardId, routing) == shardId) {
                return routing;
            }
        }
        throw new AssertionError("unable to find routing for shard " + shardId);
    }

    private static void assertLongField(FieldCapabilitiesResponse response, String field) {
        assertTrue("expected " + field + " in " + response.get().keySet(), response.get().containsKey(field));
        FieldCapabilities caps = response.get().get(field).get("long");
        assertNotNull(caps);
        assertTrue(caps.isSearchable());
        assertTrue(caps.isAggregatable());
    }

    private static void assertKeywordField(FieldCapabilitiesResponse response, String field) {
        assertTrue("expected " + field + " in " + response.get().keySet(), response.get().containsKey(field));
        FieldCapabilities caps = response.get().get(field).get("keyword");
        assertNotNull(caps);
        assertTrue(caps.isSearchable());
        assertTrue(caps.isAggregatable());
    }
}
