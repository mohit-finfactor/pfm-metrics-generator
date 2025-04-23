package com.example.demo.common;

import com.ftpl.pfm.common.model.statistics.StatisticsId;
import com.ftpl.pfm.common.model.statistics.StatisticsTable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.LocalDate;
import java.util.List;

public interface StatisticsRepository extends JpaRepository<StatisticsTable, StatisticsId> {
    @Query(nativeQuery = true, value = "select * from fiu_pfm_statistics where timestamp = (select max(timestamp) from fiu_pfm_statistics) order by metric_name ASC;")
    List<StatisticsTable> findDistinctByMetricNameInOrderByTimestampDesc();

    @Query(nativeQuery = true, value = "select distinct DATE(timestamp) as timestamp, metric_name, metric_value from fiu_pfm_statistics where metric_name = ?1 and DATE(timestamp) IN (?2) ORDER BY timestamp ASC;")
    List<StatisticsTable> findGraphMetrics(String metricName, List<LocalDate> timeIntPointsList);
}
