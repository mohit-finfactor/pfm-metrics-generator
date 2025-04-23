package com.example.demo.dashboard;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DashboardMetrics {

    private String aaId;
    private String metricName;
    private String metricValue;
    private String intervalTimeType;

}
