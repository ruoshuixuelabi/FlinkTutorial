package com.atguigu.chapter12;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderEvent {
    public String userId;
    public String orderId;
    public String eventType;
    public Long timestamp;
}