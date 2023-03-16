package com.atguigu.chapter06;

import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class CustomPunctuatedGenerator implements WatermarkGenerator<Event> {
    @Override
    public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
        //只有在遇到特定的事件时才发出水位线
        if (event.user.equals("Mary")) {
            output.emitWatermark(new Watermark(event.timestamp));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        //不需要做任何事情，因为我们在onEvent ()方法中发出了水位线
    }
}