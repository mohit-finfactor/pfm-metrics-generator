package com.example.demo;

import com.ctpl.finvu.common.consent.model.ConsentsRequestConsentDetail.FiTypesEnum;
import com.example.demo.common.*;
import com.ftpl.pfm.common.enums.ConsentSessionStatus;
import com.ftpl.pfm.common.enums.ConsentStatus;
import com.ftpl.pfm.common.model.accounts.AccountLinkStatus;
import com.ftpl.pfm.common.model.customer.SubscriptionStatusDao;
import com.ftpl.pfm.common.model.statistics.StatisticsTable;
import jakarta.annotation.PostConstruct;
import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;
import org.thymeleaf.context.Context;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.*;
import java.util.*;
import java.util.stream.Collectors;

import static com.example.demo.dashboard.MetricsName.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class PfmDashboardService {

    private static final String DEPOSITS = FiTypesEnum.DEPOSIT.toString();
    private static final String MUTUAL_FUNDS = FiTypesEnum.MUTUAL_FUNDS.toString();
    private static final String EQUITIES = FiTypesEnum.EQUITIES.toString();
    public static final String ASIA_KOLKATA = "Asia/Kolkata";
    private static final String TOTAL = "Total";

    private final CustomerAccountRepository customerAccountRepository;
    private final CustomerConsentRepository customerConsentRepository;
    private final CustomerRepository customerRepository;
    private final CustomerConsentHandleRepository customerConsentHandleRepository;
    private final JavaMailSender emailSender;
    private final ThymeleafTemplateConfig thymeleafTemplateConfig;
    private final CustomerConsentSessionRepository customerConsentSessionRepository;

    @Value("${job.start-date:2024-04-01}")
    private LocalDate startDate;

    @Value("${job.end-date:2024-04-27}")
    private LocalDate endDate;

    @Value("${email.from}")
    private String fromAddress;

    @Value("${email.to}")
    private String[] toAddresses;

    @Value("${email.subject}")
    private String subject;

    @PostConstruct
    public void init() {
        if (startDate == null || endDate == null) {
            final LocalDate defaultDate = LocalDate.now(ZoneId.of("UTC"));
            this.startDate = LocalDate.of(defaultDate.getYear(), Month.APRIL, 6);
            this.endDate = LocalDate.of(defaultDate.getYear(), Month.APRIL, 6);
        } else {
            this.startDate = startDate.atStartOfDay(ZoneId.systemDefault())
                    .withZoneSameInstant(ZoneId.of("UTC"))
                    .toLocalDate();
            this.endDate = endDate.atStartOfDay(ZoneId.systemDefault())
                    .withZoneSameInstant(ZoneId.of("UTC"))
                    .toLocalDate();
        }

        log.info("calculateDashboardStatistics - Start Date: {}", startDate);
        log.info("calculateDashboardStatistics - End Date: {}", endDate);

        for (LocalDate date = startDate; !date.isAfter(endDate); date = date.plusDays(1)) {
            final Map<String, StatisticsTable> statisticsTableDataList = calculateDashboardStatistics(date);
            final List<StatisticsTable> sortedList = statisticsTableDataList.values().stream()
                    .sorted(Comparator.comparing(StatisticsTable::getMetricName))
                    .toList();
            final SortedMap<String, List<String>> dashboardMetricsList = fetchDashboardMetrics(sortedList);

            log.trace("fetching success rate metrics");
            final SortedMap<String, List<String>> totalRowForSuccessRateMetrics = new TreeMap<>();
            final SortedMap<String, List<String>> successRateMetrics = fetchSuccessRateMetrics(totalRowForSuccessRateMetrics, date);

            log.trace("fetching account id count");
            final HashMap<String, String> totalRowForAccountIdCount = new HashMap<>();
            final HashMap<String, String> accountIdCount = fetchAccountIdCount(totalRowForAccountIdCount, date);
            sendEmail(dashboardMetricsList, totalRowForSuccessRateMetrics, successRateMetrics, totalRowForAccountIdCount, accountIdCount, date);
        }
    }

    private HashMap<String, String> fetchAccountIdCount(final HashMap<String, String> totalRow, LocalDate date) {
        final Date startDate = getStartTimeOfToday(date);
        final Date endDate = getEndTime(date);

        final HashMap<String, Float> accountIdCountYesterday = (HashMap<String, Float>) customerAccountRepository.findCountOfDistinctAccountIdGroupByFipId(AccountLinkStatus.LINKED.toString(), startDate, endDate)
                .stream().collect(Collectors.toMap((object -> object[0].toString()), (object -> Float.valueOf(object[1].toString()))));
        totalRow.put(TOTAL, String.format("%.0f", accountIdCountYesterday.values().stream().reduce(0F, Float::sum)));

        final HashMap<String, String> response = new HashMap<>();
        accountIdCountYesterday.forEach((key, value) -> response.put(key, String.format("%.0f", value)));
        return response;
    }

    private void sendEmail(final SortedMap<String, List<String>> dashboardMetrics,
                           final SortedMap<String, List<String>> totalRowForSuccessRateMetrics,
                           final SortedMap<String, List<String>> successRateMetrics,
                           final HashMap<String, String> totalRowForAccountIdCount,
                           final HashMap<String, String> accountIdCount,
                           final LocalDate date) {
        log.info("Sending Email");
        sendHtmlMessage(dashboardMetrics, totalRowForSuccessRateMetrics, successRateMetrics, totalRowForAccountIdCount, accountIdCount, date);
        log.info("Email Sent");
    }

    public Map<String, StatisticsTable> calculateDashboardStatistics(final LocalDate currentDate) {
        log.info("Started Dashboard Statistics Calculation for : {}", getStartTimeOfToday(currentDate));

        final Date currentTime = Date.from(currentDate.atStartOfDay(ZoneId.of(ASIA_KOLKATA)).toInstant());
        final Date startTimeOfToday = getStartTimeOfToday(currentDate);
        final Date endTime = getEndTime(currentDate);
        final Date startTimeOfMTD = getStartTimeOfMTD(currentDate);
        final Date startTimeOfYTD = getStartTimeOfYTD(currentDate);
        final Date startTimeOfFYTD = getStartTimeOfFYTD(currentDate);

        log.info("Current Time (Start of Today): {}", currentTime);
        log.info("Start Time of Today: {}", startTimeOfToday);
        log.info("End Time (End of Today): {}", endTime);
        log.info("Start Time of MTD (Month-To-Date): {}", startTimeOfMTD);
        log.info("Start Time of YTD (Year-To-Date): {}", startTimeOfYTD);
        log.info("Start Time of FYTD (Financial Year-To-Date): {}", startTimeOfFYTD);

        final Map<String, StatisticsTable> statisticsTableDataList = new HashMap<>();
        statisticsTableDataList.put(TotalActiveConsents,new StatisticsTable(TotalActiveConsents, String.valueOf(customerConsentRepository.countByConsentStatusEquals(ConsentStatus.ACTIVE.toString(), endTime)), currentTime));
        statisticsTableDataList.put(TotalActiveConsents1Day,new StatisticsTable(TotalActiveConsents1Day, String.valueOf(customerConsentRepository.countByConsentStatusEqualsAndCreateTimeGreaterThanEqualAndCreateTimeLessThan(ConsentStatus.ACTIVE.toString(), startTimeOfToday, endTime)), currentTime));
        statisticsTableDataList.put(TotalActiveConsents1Month, new StatisticsTable(TotalActiveConsents1Month, String.valueOf(customerConsentRepository.countByConsentStatusEqualsAndCreateTimeGreaterThanEqualAndCreateTimeLessThan(ConsentStatus.ACTIVE.toString(), startTimeOfMTD, endTime)), currentTime));
        statisticsTableDataList.put(TotalActiveConsents1Year, new StatisticsTable(TotalActiveConsents1Year, String.valueOf(customerConsentRepository.countByConsentStatusEqualsAndCreateTimeGreaterThanEqualAndCreateTimeLessThan(ConsentStatus.ACTIVE.toString(), startTimeOfYTD, endTime)), currentTime));
        statisticsTableDataList.put(TotalActiveConsents1FinancialYear, new StatisticsTable(TotalActiveConsents1FinancialYear, String.valueOf(customerConsentRepository.countByConsentStatusEqualsAndCreateTimeGreaterThanEqualAndCreateTimeLessThan(ConsentStatus.ACTIVE.toString(), startTimeOfFYTD, endTime)), currentTime));
        statisticsTableDataList.put(TotalRevokedConsents,new StatisticsTable(TotalRevokedConsents, String.valueOf(customerConsentRepository.countByConsentStatusEquals(ConsentStatus.REVOKED.toString(), endTime)), currentTime));
        statisticsTableDataList.put(TotalRevokedConsents1Day,new StatisticsTable(TotalRevokedConsents1Day, String.valueOf(customerConsentRepository.countByConsentStatusEqualsAndUpdateTimeGreaterThanEqualAndUpdateTimeLessThan(ConsentStatus.REVOKED.toString(), startTimeOfToday, endTime)), currentTime));
        statisticsTableDataList.put(TotalRevokedConsents1Month, new StatisticsTable(TotalRevokedConsents1Month, String.valueOf(customerConsentRepository.countByConsentStatusEqualsAndUpdateTimeGreaterThanEqualAndUpdateTimeLessThan(ConsentStatus.REVOKED.toString(), startTimeOfMTD, endTime)), currentTime));
        statisticsTableDataList.put(TotalRevokedConsents1Year, new StatisticsTable(TotalRevokedConsents1Year, String.valueOf(customerConsentRepository.countByConsentStatusEqualsAndUpdateTimeGreaterThanEqualAndUpdateTimeLessThan(ConsentStatus.REVOKED.toString(), startTimeOfYTD, endTime)), currentTime));
        statisticsTableDataList.put(TotalRevokedConsents1FinancialYear, new StatisticsTable(TotalRevokedConsents1FinancialYear, String.valueOf(customerConsentRepository.countByConsentStatusEqualsAndUpdateTimeGreaterThanEqualAndUpdateTimeLessThan(ConsentStatus.REVOKED.toString(), startTimeOfFYTD, endTime)), currentTime));
        statisticsTableDataList.put(TotalAccounts, new StatisticsTable(TotalAccounts, String.valueOf(customerAccountRepository.countTotalNoOfAccounts(endTime)), currentTime));
        statisticsTableDataList.put(TotalAccounts1Day, new StatisticsTable(TotalAccounts1Day, String.valueOf(customerAccountRepository.countByLinkedDateGreaterThanEqualAndLinkedDateLessThan(startTimeOfToday, endTime)), currentTime));
        statisticsTableDataList.put(TotalAccounts1Month, new StatisticsTable(TotalAccounts1Month, String.valueOf(customerAccountRepository.countByLinkedDateGreaterThanEqualAndLinkedDateLessThan(startTimeOfMTD, endTime)), currentTime));
        statisticsTableDataList.put(TotalAccounts1Year, new StatisticsTable(TotalAccounts1Year, String.valueOf(customerAccountRepository.countByLinkedDateGreaterThanEqualAndLinkedDateLessThan(startTimeOfYTD, endTime)), currentTime));
        statisticsTableDataList.put(TotalAccounts1FinancialYear, new StatisticsTable(TotalAccounts1FinancialYear, String.valueOf(customerAccountRepository.countByLinkedDateGreaterThanEqualAndLinkedDateLessThan(startTimeOfFYTD, endTime)), currentTime));
        statisticsTableDataList.put(CurrentlyLinkedAccounts, new StatisticsTable(CurrentlyLinkedAccounts, String.valueOf(customerAccountRepository.countByLinkStatusEquals(AccountLinkStatus.LINKED.toString(), endTime)), currentTime));
        statisticsTableDataList.put(CurrentlyLinkedAccounts1Day, new StatisticsTable(CurrentlyLinkedAccounts1Day, String.valueOf(customerAccountRepository.countByLinkStatusEqualsAndLinkedDateGreaterThanEqualAndLinkedDateLessThan(AccountLinkStatus.LINKED.toString(), startTimeOfToday, endTime)), currentTime));
        statisticsTableDataList.put(CurrentlyLinkedAccounts1Month, new StatisticsTable(CurrentlyLinkedAccounts1Month, String.valueOf(customerAccountRepository.countByLinkStatusEqualsAndLinkedDateGreaterThanEqualAndLinkedDateLessThan(AccountLinkStatus.LINKED.toString(), startTimeOfMTD, endTime)), currentTime));
        statisticsTableDataList.put(CurrentlyLinkedAccounts1Year, new StatisticsTable(CurrentlyLinkedAccounts1Year, String.valueOf(customerAccountRepository.countByLinkStatusEqualsAndLinkedDateGreaterThanEqualAndLinkedDateLessThan(AccountLinkStatus.LINKED.toString(), startTimeOfYTD, endTime)), currentTime));
        statisticsTableDataList.put(CurrentlyLinkedAccounts1FinancialYear, new StatisticsTable(CurrentlyLinkedAccounts1FinancialYear, String.valueOf(customerAccountRepository.countByLinkStatusEqualsAndLinkedDateGreaterThanEqualAndLinkedDateLessThan(AccountLinkStatus.LINKED.toString(), startTimeOfFYTD, endTime)), currentTime));
        statisticsTableDataList.put(TotalCustomersRegistered, new StatisticsTable(TotalCustomersRegistered, String.valueOf(customerRepository.countTotalNoOfCustomersRegistered(endTime)), currentTime));
        statisticsTableDataList.put(TotalCustomersRegistered1Day, new StatisticsTable(TotalCustomersRegistered1Day, String.valueOf(customerRepository.countByCreateTimeGreaterThanEqualAndCreateTimeLessThan(startTimeOfToday, endTime)), currentTime));
        statisticsTableDataList.put(TotalCustomersRegistered1Month, new StatisticsTable(TotalCustomersRegistered1Month, String.valueOf(customerRepository.countByCreateTimeGreaterThanEqualAndCreateTimeLessThan(startTimeOfMTD, endTime)), currentTime));
        statisticsTableDataList.put(TotalCustomersRegistered1Year, new StatisticsTable(TotalCustomersRegistered1Year, String.valueOf(customerRepository.countByCreateTimeGreaterThanEqualAndCreateTimeLessThan(startTimeOfYTD, endTime)), currentTime));
        statisticsTableDataList.put(TotalCustomersRegistered1FinancialYear, new StatisticsTable(TotalCustomersRegistered1FinancialYear, String.valueOf(customerRepository.countByCreateTimeGreaterThanEqualAndCreateTimeLessThan(startTimeOfFYTD, endTime)), currentTime));
        statisticsTableDataList.put(ActiveSubscriptions, new StatisticsTable(ActiveSubscriptions, String.valueOf(customerRepository.countBySubscriptionStatusEquals(SubscriptionStatusDao.Y.toString(), endTime)), currentTime));
        statisticsTableDataList.put(ActiveSubscriptions1Day, new StatisticsTable(ActiveSubscriptions1Day, String.valueOf(customerRepository.countBySubscriptionStatusEqualsAndUpdateTimeGreaterThanEqualAndUpdateTimeLessThan(SubscriptionStatusDao.Y.toString(), startTimeOfToday, endTime)), currentTime));
        statisticsTableDataList.put(ActiveSubscriptions1Month, new StatisticsTable(ActiveSubscriptions1Month, String.valueOf(customerRepository.countBySubscriptionStatusEqualsAndUpdateTimeGreaterThanEqualAndUpdateTimeLessThan(SubscriptionStatusDao.Y.toString(), startTimeOfMTD, endTime)), currentTime));
        statisticsTableDataList.put(ActiveSubscriptions1Year, new StatisticsTable(ActiveSubscriptions1Year, String.valueOf(customerRepository.countBySubscriptionStatusEqualsAndUpdateTimeGreaterThanEqualAndUpdateTimeLessThan(SubscriptionStatusDao.Y.toString(), startTimeOfYTD, endTime)), currentTime));
        statisticsTableDataList.put(ActiveSubscriptions1FinancialYear, new StatisticsTable(ActiveSubscriptions1FinancialYear, String.valueOf(customerRepository.countBySubscriptionStatusEqualsAndUpdateTimeGreaterThanEqualAndUpdateTimeLessThan(SubscriptionStatusDao.Y.toString(), startTimeOfFYTD, endTime)), currentTime));
        statisticsTableDataList.put(InactiveSubscriptions, new StatisticsTable(InactiveSubscriptions, String.valueOf(customerRepository.countBySubscriptionStatusEquals(SubscriptionStatusDao.N.toString(), endTime)), currentTime));
        statisticsTableDataList.put(InactiveSubscriptions1Day, new StatisticsTable(InactiveSubscriptions1Day, String.valueOf(customerRepository.countBySubscriptionStatusEqualsAndUpdateTimeGreaterThanEqualAndUpdateTimeLessThan(SubscriptionStatusDao.N.toString(), startTimeOfToday, endTime)), currentTime));
        statisticsTableDataList.put(InactiveSubscriptions1Month, new StatisticsTable(InactiveSubscriptions1Month, String.valueOf(customerRepository.countBySubscriptionStatusEqualsAndUpdateTimeGreaterThanEqualAndUpdateTimeLessThan(SubscriptionStatusDao.N.toString(), startTimeOfMTD, endTime)), currentTime));
        statisticsTableDataList.put(InactiveSubscriptions1Year, new StatisticsTable(InactiveSubscriptions1Year, String.valueOf(customerRepository.countBySubscriptionStatusEqualsAndUpdateTimeGreaterThanEqualAndUpdateTimeLessThan(SubscriptionStatusDao.N.toString(), startTimeOfYTD, endTime)), currentTime));
        statisticsTableDataList.put(InactiveSubscriptions1FinancialYear, new StatisticsTable(InactiveSubscriptions1FinancialYear, String.valueOf(customerRepository.countBySubscriptionStatusEqualsAndUpdateTimeGreaterThanEqualAndUpdateTimeLessThan(SubscriptionStatusDao.N.toString(), startTimeOfFYTD, endTime)), currentTime));
        statisticsTableDataList.put(ExpiringConsents15Day, new StatisticsTable(ExpiringConsents15Day, String.valueOf(customerConsentHandleRepository.countByConsentFiDataRangeToGreaterThanEqualAndConsentFiDataRangeToLessThan(Date.from(currentDate.atStartOfDay().toInstant(ZoneOffset.ofHoursMinutes(5,30))), Date.from(currentDate.plusDays(16).atStartOfDay().toInstant(ZoneOffset.ofHoursMinutes(5,30))))), currentTime));
        statisticsTableDataList.put(ExpiringSubscription15Day, new StatisticsTable(ExpiringSubscription15Day, String.valueOf(customerRepository.countBySubscriptionEndGreaterThanEqualAndSubscriptionEndLessThan(Date.from(currentDate.atStartOfDay().toInstant(ZoneOffset.ofHoursMinutes(5,30))), Date.from(currentDate.plusDays(16).atStartOfDay().toInstant(ZoneOffset.ofHoursMinutes(5,30))))), currentTime));

        final long totalUniqueUsers = customerAccountRepository.totalUniqueUsers(endTime);
        statisticsTableDataList.put(TotalUniqueUsers, new StatisticsTable(TotalUniqueUsers, String.valueOf(totalUniqueUsers), currentTime));

        final long totalUniqueUsersInLast1Day = customerAccountRepository.totalUniqueUsersInDateRange(startTimeOfToday, endTime);
        statisticsTableDataList.put(TotalUniqueUsers1Day, new StatisticsTable(TotalUniqueUsers1Day, String.valueOf(totalUniqueUsersInLast1Day), currentTime));

        final long totalUniqueUsersInLast1Month = customerAccountRepository.totalUniqueUsersInDateRange(startTimeOfMTD, endTime);
        statisticsTableDataList.put(TotalUniqueUsers1Month, new StatisticsTable(TotalUniqueUsers1Month, String.valueOf(totalUniqueUsersInLast1Month), currentTime));

        final long totalUniqueUsersInLast1Year = customerAccountRepository.totalUniqueUsersInDateRange(startTimeOfYTD, endTime);
        statisticsTableDataList.put(TotalUniqueUsers1Year, new StatisticsTable(TotalUniqueUsers1Year, String.valueOf(totalUniqueUsersInLast1Year), currentTime));

        final long totalUniqueUsersInLast1FinancialYear = customerAccountRepository.totalUniqueUsersInDateRange(startTimeOfFYTD, endTime);
        statisticsTableDataList.put(TotalUniqueUsers1FinancialYear, new StatisticsTable(TotalUniqueUsers1FinancialYear, String.valueOf(totalUniqueUsersInLast1FinancialYear), currentTime));


        final long totalUniqueUsersPerDeposit = customerAccountRepository.totalUniqueUsersPerFiType(DEPOSITS, endTime);
        statisticsTableDataList.put(TotalUsersDeposits, new StatisticsTable(TotalUsersDeposits, String.valueOf(totalUniqueUsersPerDeposit), currentTime));

        final long totalUniqueUsersPerDepositsInLast1Day = customerAccountRepository.totalUniqueUsersPerFiTypeInDateRange(DEPOSITS, startTimeOfToday, endTime);
        statisticsTableDataList.put(TotalUsersDeposits1Day, new StatisticsTable(TotalUsersDeposits1Day, String.valueOf(totalUniqueUsersPerDepositsInLast1Day), currentTime));

        final long totalUniqueUsersPerDepositsInLast1Month = customerAccountRepository.totalUniqueUsersPerFiTypeInDateRange(DEPOSITS, startTimeOfMTD, endTime);
        statisticsTableDataList.put(TotalUsersDeposits1Month, new StatisticsTable(TotalUsersDeposits1Month, String.valueOf(totalUniqueUsersPerDepositsInLast1Month), currentTime));

        final long totalUniqueUsersPerDepositsInLast1Year = customerAccountRepository.totalUniqueUsersPerFiTypeInDateRange(DEPOSITS, startTimeOfYTD, endTime);
        statisticsTableDataList.put(TotalUsersDeposits1Year, new StatisticsTable(TotalUsersDeposits1Year, String.valueOf(totalUniqueUsersPerDepositsInLast1Year), currentTime));

        final long totalUniqueUsersPerDepositsInLast1FinancialYear = customerAccountRepository.totalUniqueUsersPerFiTypeInDateRange(DEPOSITS, startTimeOfFYTD, endTime);
        statisticsTableDataList.put(TotalUsersDeposits1FinancialYear, new StatisticsTable(TotalUsersDeposits1FinancialYear, String.valueOf(totalUniqueUsersPerDepositsInLast1FinancialYear), currentTime));

        final long totalUniqueUsersPerEquities = customerAccountRepository.totalUniqueUsersPerFiType(EQUITIES, endTime);
        statisticsTableDataList.put(TotalUsersEquities, new StatisticsTable(TotalUsersEquities, String.valueOf(totalUniqueUsersPerEquities), currentTime));

        final long totalUniqueUsersPerEquitiesInLast1Day = customerAccountRepository.totalUniqueUsersPerFiTypeInDateRange(EQUITIES, startTimeOfToday, endTime);
        statisticsTableDataList.put(TotalUsersEquities1Day, new StatisticsTable(TotalUsersEquities1Day, String.valueOf(totalUniqueUsersPerEquitiesInLast1Day), currentTime));

        final long totalUniqueUsersPerEquitiesInLast1Month = customerAccountRepository.totalUniqueUsersPerFiTypeInDateRange(EQUITIES, startTimeOfMTD, endTime);
        statisticsTableDataList.put(TotalUsersEquities1Month, new StatisticsTable(TotalUsersEquities1Month, String.valueOf(totalUniqueUsersPerEquitiesInLast1Month), currentTime));

        final long totalUniqueUsersPerEquitiesInLast1Year = customerAccountRepository.totalUniqueUsersPerFiTypeInDateRange(EQUITIES, startTimeOfYTD, endTime);
        statisticsTableDataList.put(TotalUsersEquities1Year, new StatisticsTable(TotalUsersEquities1Year, String.valueOf(totalUniqueUsersPerEquitiesInLast1Year), currentTime));

        final long totalUniqueUsersPerEquitiesInLast1FinancialYear = customerAccountRepository.totalUniqueUsersPerFiTypeInDateRange(EQUITIES, startTimeOfFYTD, endTime);
        statisticsTableDataList.put(TotalUsersEquities1FinancialYear, new StatisticsTable(TotalUsersEquities1FinancialYear, String.valueOf(totalUniqueUsersPerEquitiesInLast1FinancialYear), currentTime));

        final long totalUniqueUsersPerMutualFunds = customerAccountRepository.totalUniqueUsersPerFiType(MUTUAL_FUNDS, endTime);
        statisticsTableDataList.put(TotalUsersMutualFunds, new StatisticsTable(TotalUsersMutualFunds, String.valueOf(totalUniqueUsersPerMutualFunds), currentTime));

        final long totalUniqueUsersPerMutualFundsInLast1Day = customerAccountRepository.totalUniqueUsersPerFiTypeInDateRange(MUTUAL_FUNDS, startTimeOfToday, endTime);
        statisticsTableDataList.put(TotalUsersMutualFunds1Day, new StatisticsTable(TotalUsersMutualFunds1Day, String.valueOf(totalUniqueUsersPerMutualFundsInLast1Day), currentTime));

        final long totalUniqueUsersPerMutualFundsInLast1Month = customerAccountRepository.totalUniqueUsersPerFiTypeInDateRange(MUTUAL_FUNDS, startTimeOfMTD, endTime);
        statisticsTableDataList.put(TotalUsersMutualFunds1Month, new StatisticsTable(TotalUsersMutualFunds1Month, String.valueOf(totalUniqueUsersPerMutualFundsInLast1Month), currentTime));

        final long totalUniqueUsersPerMutualFundsInLast1Year = customerAccountRepository.totalUniqueUsersPerFiTypeInDateRange(MUTUAL_FUNDS, startTimeOfYTD, endTime);
        statisticsTableDataList.put(TotalUsersMutualFunds1Year, new StatisticsTable(TotalUsersMutualFunds1Year, String.valueOf(totalUniqueUsersPerMutualFundsInLast1Year), currentTime));

        final long totalUniqueUsersPerMutualFundsInLast1FinancialYear = customerAccountRepository.totalUniqueUsersPerFiTypeInDateRange(MUTUAL_FUNDS, startTimeOfFYTD, endTime);
        statisticsTableDataList.put(TotalUsersDeposits1FinancialYear, new StatisticsTable(TotalUsersDeposits1FinancialYear, String.valueOf(totalUniqueUsersPerMutualFundsInLast1FinancialYear), currentTime));

        log.info("Ending Dashboard Statistics Calculation for : {}", getStartTimeOfToday(currentDate));

        return statisticsTableDataList;

    }

    public void writeStatisticsToCSV(final Map<String, StatisticsTable> statisticsList) {
        final String filePath = "./metrics.csv";

        try (final BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {  // `true` enables append mode

            // Check if the file is empty to write headers only once
            if (Files.size(Paths.get(filePath)) == 0) {
                writer.write("metric_name,metric_value,timestamp");
                writer.newLine();
            }

            for (final StatisticsTable stat : statisticsList.values()) {
                final String row = String.format("%s,%s,%s",
                        stat.getMetricName(),
                        stat.getMetricValue(),
                        stat.getTimestamp()
                );
                writer.write(row);
                writer.newLine();
            }

            log.info("Successfully appended statistics to CSV: {}", filePath);

        } catch (final IOException e) {
            log.error("Error while writing statistics to CSV", e);
        }
    }



    private Date getStartTimeOfFYTD(final LocalDate currentDate) {
        final LocalDate fyStartDate;
        if (currentDate.getMonthValue() < 4 ||
                (currentDate.getMonthValue() == 4 && currentDate.getDayOfMonth() == 1)) {
            fyStartDate = LocalDate.of(currentDate.getYear() - 1, 4, 1);
        } else {
            fyStartDate = LocalDate.of(currentDate.getYear(), 4, 1);
        }
        return Date.from(fyStartDate.atStartOfDay(ZoneId.of(ASIA_KOLKATA)).toInstant());
    }

    private Date getStartTimeOfToday(final LocalDate yesterday) {
        return Date.from(yesterday.atStartOfDay(ZoneId.of(ASIA_KOLKATA)).toInstant());
    }

    private Date getStartTimeOfYTD(final LocalDate currentDate) {
        final LocalDate ytdStartDate;
        if (currentDate.getDayOfYear() == 1) {
            ytdStartDate = LocalDate.of(currentDate.getYear() - 1, 1, 1);
        } else {
            ytdStartDate = LocalDate.of(currentDate.getYear(), 1, 1);
        }
        return Date.from(ytdStartDate.atStartOfDay(ZoneId.of(ASIA_KOLKATA)).toInstant());
    }

    private Date getStartTimeOfMTD(final LocalDate currentDate) {
        final LocalDate mtdStartDate;
        if (currentDate.getDayOfMonth() == 1) {
            final LocalDate prevMonth = currentDate.minusMonths(1);
            mtdStartDate = LocalDate.of(prevMonth.getYear(), prevMonth.getMonth(), 1);
        } else {
            mtdStartDate = LocalDate.of(currentDate.getYear(), currentDate.getMonth(), 1);
        }
        return Date.from(mtdStartDate.atStartOfDay(ZoneId.of(ASIA_KOLKATA)).toInstant());
    }

    private Date getEndTime(LocalDate currentDate) {
        currentDate = currentDate.plusDays(1);
        final LocalDateTime endTime = currentDate.atStartOfDay(ZoneId.of(ASIA_KOLKATA)).toLocalDateTime();
        return Date.from(endTime.atZone(ZoneId.of(ASIA_KOLKATA)).toInstant());
    }

    private SortedMap<String, List<String>> fetchDashboardMetrics(final List<StatisticsTable> metricsList) {
        final SortedMap<String, List<String>> dailyMetricTableHashMap = new TreeMap<>();
        metricsList.forEach(metric -> {
            if (metric.getMetricName().contains("active_subscriptions") && metric.getMetricName().startsWith("active")) {
                if (dailyMetricTableHashMap.containsKey(ActiveSubscriptions))
                    dailyMetricTableHashMap.get(ActiveSubscriptions).add(metric.getMetricValue());
                else
                    dailyMetricTableHashMap.put(ActiveSubscriptions, new ArrayList<>(List.of(metric.getMetricValue())));
            } else if (metric.getMetricName().contains("currently_linked_accounts")) {
                if (dailyMetricTableHashMap.containsKey(CurrentlyLinkedAccounts))
                    dailyMetricTableHashMap.get(CurrentlyLinkedAccounts).add(metric.getMetricValue());
                else
                    dailyMetricTableHashMap.put(CurrentlyLinkedAccounts, new ArrayList<>(List.of(metric.getMetricValue())));
            } else if (metric.getMetricName().contains(TotalUsersDeposits)) {
                dailyMetricTableHashMap.computeIfAbsent(TotalUsersDeposits, k -> new ArrayList<>()).add(metric.getMetricValue());
            } else if (metric.getMetricName().contains(TotalUsersEquities)) {
                dailyMetricTableHashMap.computeIfAbsent(TotalUsersEquities, k -> new ArrayList<>()).add(metric.getMetricValue());
            } else if (metric.getMetricName().contains(TotalUsersMutualFunds)) {
                dailyMetricTableHashMap.computeIfAbsent(TotalUsersMutualFunds, k -> new ArrayList<>()).add(metric.getMetricValue());
            } else if (metric.getMetricName().contains(TotalUniqueUsers)) {
                dailyMetricTableHashMap.computeIfAbsent(TotalUniqueUsers, k -> new ArrayList<>()).add(metric.getMetricValue());
            } else if (metric.getMetricName().contains("inactive_subscriptions")) {
                if (dailyMetricTableHashMap.containsKey(InactiveSubscriptions))
                    dailyMetricTableHashMap.get(InactiveSubscriptions).add(metric.getMetricValue());
                else
                    dailyMetricTableHashMap.put(InactiveSubscriptions, new ArrayList<>(List.of(metric.getMetricValue())));
            } else if (metric.getMetricName().contains("total_accounts")) {
                if (dailyMetricTableHashMap.containsKey(TotalAccounts))
                    dailyMetricTableHashMap.get(TotalAccounts).add(metric.getMetricValue());
                else
                    dailyMetricTableHashMap.put(TotalAccounts, new ArrayList<>(List.of(metric.getMetricValue())));
            } else if (metric.getMetricName().contains("total_active_consents")){
                if (dailyMetricTableHashMap.containsKey(TotalActiveConsents))
                    dailyMetricTableHashMap.get(TotalActiveConsents).add(metric.getMetricValue());
                else
                    dailyMetricTableHashMap.put(TotalActiveConsents, new ArrayList<>(List.of(metric.getMetricValue())));
            } else if (metric.getMetricName().contains("total_revoked_consents")){
                if (dailyMetricTableHashMap.containsKey(TotalRevokedConsents))
                    dailyMetricTableHashMap.get(TotalRevokedConsents).add(metric.getMetricValue());
                else
                    dailyMetricTableHashMap.put(TotalRevokedConsents, new ArrayList<>(List.of(metric.getMetricValue())));
            }else if (metric.getMetricName().contains("total_customers_registered")) {
                if (dailyMetricTableHashMap.containsKey(TotalCustomersRegistered))
                    dailyMetricTableHashMap.get(TotalCustomersRegistered).add(metric.getMetricValue());
                else
                    dailyMetricTableHashMap.put(TotalCustomersRegistered, new ArrayList<>(List.of(metric.getMetricValue())));
            } else if (metric.getMetricName().contains("expiring_consents")) {
                dailyMetricTableHashMap.put(ExpiringConsents15Day, new ArrayList<>(List.of(new String[]{"-", "-", "-", "-"})));
                dailyMetricTableHashMap.get(ExpiringConsents15Day).add(0, metric.getMetricValue());
            } else if (metric.getMetricName().contains("expiring_subscription")) {
                dailyMetricTableHashMap.put(ExpiringSubscription15Day, new ArrayList<>(List.of(new String[]{"-", "-", "-", "-"})));
                dailyMetricTableHashMap.get(ExpiringSubscription15Day).add(0, metric.getMetricValue());
            }
        });

        return dailyMetricTableHashMap;
    }


    private void sendHtmlMessage(final SortedMap<String, List<String>> dashboardMetrics,
                                 final SortedMap<String, List<String>> totalRowForSuccessRateMetrics,
                                 final SortedMap<String, List<String>> successRateMetrics,
                                 final HashMap<String, String> totalRowForAccountIdCount,
                                 final HashMap<String, String> accountIdCount,
                                 final LocalDate date) {
        try {
            final MimeMessage message = emailSender.createMimeMessage();
            final MimeMessageHelper helper = new MimeMessageHelper(message, MimeMessageHelper.MULTIPART_MODE_MIXED_RELATED, StandardCharsets.UTF_8.name());
            final Context context = new Context();
            context.setVariable("dailyMetricsTableData", dashboardMetrics);
            context.setVariable("dailyMetricsTableData", dashboardMetrics);
            context.setVariable("successRateMetrics", successRateMetrics);
            context.setVariable("accountIdCount", accountIdCount);
            context.setVariable("totalRowForSuccessMetrics", totalRowForSuccessRateMetrics);
            context.setVariable("totalRowForAccountIdCount", totalRowForAccountIdCount);

            context.setVariable("calculatedDate", date);
            helper.setFrom(fromAddress);
            helper.setTo(toAddresses);
            helper.setSubject(subject);

            log.trace(Arrays.toString(message.getFrom()));
            log.trace(Arrays.toString(message.getAllRecipients()));
            log.trace(message.getSubject());

            final String html = thymeleafTemplateConfig.springTemplateEngine().process("emailTemplate.html", context);
            helper.setText(html, true);

            emailSender.send(message);
            log.trace("email sent successfully : {}", message);
        } catch (final Exception ex) {
            log.error("Error while sending email",ex);
        }
    }

    public SortedMap<String, List<String>> fetchSuccessRateMetrics(final SortedMap<String, List<String>> totalRow,
                                                                   final LocalDate currentDate) {
        final Date startDate = getStartTimeOfToday(currentDate);
        final Date endDate = getEndTime(currentDate);

        final Map<String, Float> totalAccountCounts = customerConsentSessionRepository.findCountOfAccountIdGroupByFIP(startDate, endDate)
                .stream()
                .filter(object -> object[0] != null)
                .collect(Collectors.toMap(
                        object -> object[0].toString(),
                        object -> Float.valueOf(object[1].toString())
                ));

        final Map<String, Float> successfulAccountCounts = customerConsentSessionRepository.findBySessionStatusCountOfAccountIdGroupByFIP(
                        ConsentSessionStatus.READY.toString(), startDate, endDate)
                .stream()
                .filter(object -> object[0] != null)
                .collect(Collectors.toMap(
                        object -> object[0].toString(),
                        object -> Float.valueOf(object[1].toString())
                ));


        final SortedMap<String, List<String>> response = new TreeMap<>();
        totalAccountCounts.forEach((fipId, count) -> {
            response.put(fipId, new ArrayList<>());
            response.get(fipId).add(String.format("%.0f", count));
            response.get(fipId).add(String.format("%.0f", successfulAccountCounts.getOrDefault(fipId, 0F)));
            response.get(fipId).add(String.format("%.2f", (successfulAccountCounts.getOrDefault(fipId, 0F).doubleValue() / count.doubleValue()) * 100));
        });

        totalRow.put(TOTAL, new ArrayList<>());
        totalRow.get(TOTAL).add(String.format("%.0f", totalAccountCounts.values().stream().reduce(0F, Float::sum)));
        totalRow.get(TOTAL).add(String.format("%.0f", successfulAccountCounts.values().stream().reduce(0F, Float::sum)));
        totalRow.get(TOTAL).add(String.format("%.2f", (successfulAccountCounts.values().stream().reduce(0F, Float::sum) / totalAccountCounts.values().stream().reduce(0F, Float::sum)) * 100));

        return response;
    }
}
