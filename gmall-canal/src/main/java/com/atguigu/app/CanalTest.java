package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalTest {

    public static void main(String[] args) {

        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        while (true) {

            canalConnector.connect();
            canalConnector.subscribe("gmall.*");

            Message message = canalConnector.get(100);

            if (message.getEntries().size() == 0) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {

                for (CanalEntry.Entry entry : message.getEntries()) {

                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {
                        String tableName = entry.getHeader().getTableName();

                        try {
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                            CanalEntry.EventType eventType = rowChange.getEventType();
                            List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();

                            handler(tableName, eventType, rowDataList);

                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }

            }


        }

    }

    private static void handler(String tableName, CanalEntry.EventType entryType, List<CanalEntry.RowData> rowDataList) {

        if ("order_info".equals(tableName)) {

            //取下单数据
            if (entryType.equals(CanalEntry.EventType.INSERT)) {

                for (CanalEntry.RowData rowData : rowDataList) {
                    JSONObject jsonObject = new JSONObject();
                    for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                        jsonObject.put(column.getName(), column.getValue());
                    }

                    System.out.println(jsonObject.toString());
                }

            }

        }

    }

}
