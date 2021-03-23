package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {

    public long getTotalDau(String date);

    public List<Map> selectDauTotalHourMap(String date);

}