package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.constants.GmallConstants;
import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.jcodings.util.Hash;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    @Autowired
    private JestClient jestClient;

    @Override
    public Long getTotalDau(String date) {
        return dauMapper.getTotalDau(date);
    }

    @Override
    public Map getHourDau(String date) {

        //定义返回值类型
        HashMap<String, Long> hourDauMap = new HashMap<>();
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        for (Map map : list) {
            hourDauMap.put((String) map.get("LOGHOUR"), (long) map.get("CT"));
        }

        return hourDauMap;
    }

    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHourMap(String date) {

        //定义返回值类型
        HashMap<String, Double> hourAmountMap = new HashMap<>();
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        for (Map map : list) {
            String lh = (String) map.get("CREATE_HOUR");
            Double ct = (Double) map.get("SUM_AMOUNT");
            System.out.println(lh + "----" + ct);
            hourAmountMap.put(lh, ct);
        }

        return hourAmountMap;
    }

    @Override
    public Map getSaleDetail(String date, String keyWord, int pageNum, int pageSize) {

        //查询语句
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //添加过滤条件
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //添加时间的过滤条件
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        //添加匹配的过滤条件
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyWord).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //聚合组
        //性别聚合组
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_gender").field("user_gender").size(2);
        //年龄聚合组
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(genderAggs);
        searchSourceBuilder.aggregation(ageAggs);

        //分页
        searchSourceBuilder.from((pageNum - 1) * pageSize);
        searchSourceBuilder.size(pageSize);

        HashMap resultMap = new HashMap();
        try {
            SearchResult searchResult = jestClient.execute(new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.ES_GMALL_SALE_DETAIL).addType("_doc").build());

            //获取总数
            Long total = searchResult.getTotal();

            //获取明细
            List<Map> detailList = new ArrayList<>();
            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
            for (SearchResult.Hit<Map, Void> hit : hits) {
                detailList.add(hit.source);
            }

            //获取聚合组数据
            HashMap genderMap = new HashMap<String, Long>();
            List<TermsAggregation.Entry> groupby_gender = searchResult.getAggregations().getTermsAggregation("groupby_gender").getBuckets();
            for (TermsAggregation.Entry entry : groupby_gender) {
                genderMap.put(entry.getKey(), entry.getCount());
            }

            HashMap ageMap = new HashMap<String, Long>();
            List<TermsAggregation.Entry> groupby_age = searchResult.getAggregations().getTermsAggregation("groupby_age").getBuckets();

            for (TermsAggregation.Entry entry : groupby_age) {
                ageMap.put(entry.getKey(), entry.getCount());
            }

            resultMap.put("total", total);
            resultMap.put("ageMap", ageMap);
            resultMap.put("genderMap", genderMap);
            resultMap.put("detail", detailList);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return resultMap;
    }
}