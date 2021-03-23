package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.bean.Option;
import com.atguigu.gmallpublisher.bean.Stat;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getTotal(@RequestParam("date") String date) {

        //获取日活数据
        Long totalDau = publisherService.getTotalDau(date);

        //获取交易额数据
        Double orderAmountTotal = publisherService.getOrderAmountTotal(date);

        //定义返回值类型
        List<Map> result = new ArrayList<>();
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", totalDau);
        result.add(dauMap);

        HashMap<String, Object> newMiDMap = new HashMap<>();
        newMiDMap.put("id", "new_mid");
        newMiDMap.put("name", "新增设备");
        newMiDMap.put("value", 233);
        result.add(newMiDMap);

        HashMap<String, Object> totalAmountMap = new HashMap<>();
        totalAmountMap.put("id", "order_amount");
        totalAmountMap.put("name", "新增交易额");
        totalAmountMap.put("value", orderAmountTotal);
        result.add(totalAmountMap);

        return JSON.toJSONString(result);
    }

    @GetMapping("realtime-hours")
    public String getHourDau(@RequestParam("id") String id, @RequestParam("date") String today) {

        if ("dau".equals(id)) {

            JSONObject jsonObject = new JSONObject();

            //查询日活的分时统计
            Map todayHourDau = publisherService.getHourDau(today);
            jsonObject.put("today", todayHourDau);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

            try {
                //获取昨天日期
                Date yesterdayDate = DateUtils.addDays(sdf.parse(today), -1);
                String yesterdayStr = sdf.format(yesterdayDate);

                Map yesterdayDauMap = publisherService.getHourDau(yesterdayStr);
                jsonObject.put("yesterday", yesterdayDauMap);

            } catch (ParseException e) {
                e.printStackTrace();
            }

            return jsonObject.toJSONString();

        } else if ("order_amount".equals(id)) {

            JSONObject jsonObject = new JSONObject();

            //查询日活的分时统计
            Map todayHourAmount = publisherService.getOrderAmountHourMap(today);
            jsonObject.put("today", todayHourAmount);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

            try {

                //获取昨天日期
                Date yesterdayDate = DateUtils.addDays(sdf.parse(today), -1);
                String yesterdayStr = sdf.format(yesterdayDate);

                Map yesterdayAmountMap = publisherService.getOrderAmountHourMap(yesterdayStr);
                jsonObject.put("yesterday", yesterdayAmountMap);

            } catch (ParseException e) {
                e.printStackTrace();
            }

            return jsonObject.toJSONString();
        }
        return null;
    }

    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date, @RequestParam("startpage") int pageNum, @RequestParam("size") int pageSize, @RequestParam("keyword") String keyword) {

        HashMap resultMap = new HashMap();

        Map saleDetail = publisherService.getSaleDetail(date, keyword, pageNum, pageSize);

        //取出数据
        Long total = (Long) saleDetail.get("total");
        Map ageMap = (Map) saleDetail.get("ageMap");
        Map genderMap = (Map) saleDetail.get("genderMap");
        List detailList = (List) saleDetail.get("detail");

        ArrayList<Stat> stats = new ArrayList<>();

        //提取性别数据
        Long femaleCount = (Long) genderMap.get("F");
        Long maleCount = (Long) genderMap.get("M");
        Double femaleRatio = Math.round(femaleCount * 1000D / total) / 10D;
        Double maleRatio = Math.round(maleCount * 1000D / total) / 10D;
        ArrayList<Option> genderOptions = new ArrayList<>();
        genderOptions.add(new Option("男", maleRatio));
        genderOptions.add(new Option("女", femaleRatio));

        //存放性别数据
        stats.add(new Stat("用户性别占比", genderOptions));

        //提取年龄数据
        Long age_20Count = 0L;
        Long age20_30Count = 0L;
        Long age30_Count = 0L;
        for (Object o : ageMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;

            //获取年龄
            int age = Integer.parseInt((String) entry.getKey());
            Long ageCount = (Long) entry.getValue();

            //判断年龄范围
            if (age < 20) {
                age_20Count += ageCount;
            } else if (age <= 30) {
                age20_30Count += ageCount;
            } else {
                age30_Count += ageCount;
            }
        }

        Double age_20Ratio = Math.round(age_20Count * 1000D / total) / 10D;
        Double age20_30Ratio = Math.round(age20_30Count * 1000D / total) / 10D;
        Double age30_Ratio = Math.round(age30_Count * 1000D / total) / 10D;

        ArrayList<Option> ageOptions = new ArrayList<>();
        ageOptions.add(new Option("20岁以下",age_20Ratio));
        ageOptions.add(new Option("20岁到30岁",age20_30Ratio));
        ageOptions.add(new Option("30岁以上",age30_Ratio));

        stats.add(new Stat("用户年龄占比",ageOptions));

        resultMap.put("total", total);
        resultMap.put("stat", stats);
        resultMap.put("detail", detailList);
        return JSON.toJSONString(resultMap);
    }

}
