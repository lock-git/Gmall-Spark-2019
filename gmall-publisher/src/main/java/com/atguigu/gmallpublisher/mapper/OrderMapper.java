package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {

    Double selectOrderAmountTotal(String date);

    List<Map> selectOrderAmountHourMap(String date);

}
