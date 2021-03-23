package com.atguigu.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {

    Long getTotalDau(String date);

    Map getHourDau(String date);

    Double getOrderAmountTotal(String date);

    Map getOrderAmountHourMap(String date);

    Map getSaleDetail(String date, String keyWord, int pageNum, int pageSize);

}