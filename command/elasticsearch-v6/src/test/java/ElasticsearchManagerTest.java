public class ElasticsearchManagerTest {

//    @Test
//    public void testElasticsearchConnection() throws IOException {
//        List<DocRequest> indexRequestList = new LinkedList<>();
//        ElasticsearchManager manager = new ElasticsearchManager("10.4.20.3", 9200, "doc");
////        manager.getClient().indices().create(new CreateIndexRequest("test"), RequestOptions.DEFAULT);
//        for (int i = 1; i < 10; i++) {
//            indexRequestList.add(new DocRequest("test", "doc", "fakeid"+i, "{\"name\":\"nan" + i + "\"}"));
//            if (indexRequestList.size() == 5) {
//                manager.doUpsert(indexRequestList);
//                indexRequestList.clear();
//            }
//        }
//        manager.doUpsert(indexRequestList);
//
//        for (int i = 1; i < 10; i++) {
//            GetResponse response = manager.getClient().get(new GetRequest("test").id("fakeid" + i), RequestOptions.DEFAULT);
//            System.out.println(response);
//        }
//    }
}