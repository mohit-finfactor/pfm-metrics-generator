package com.example.demo.common;

import com.ctpl.finvu.common.consent.model.ConsentsRequestConsentDetail.FiTypesEnum;
import com.ftpl.pfm.common.model.accounts.AccountLinkStatus;
import com.ftpl.pfm.common.model.accounts.CustomerAccountId;
import com.ftpl.pfm.common.model.accounts.CustomerAccountTable;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Collection;
import java.util.Date;
import java.util.List;

public interface CustomerAccountRepository extends JpaRepository<CustomerAccountTable, CustomerAccountId> {

	List<CustomerAccountTable> findByUniqueIdentifierAndLinkStatus(String uniqueIdentifier, AccountLinkStatus linkStatus);
	List<CustomerAccountTable> findByUniqueIdentifierAndLinkStatusAndFiType(String uniqueIdentifier, AccountLinkStatus linkStatus, FiTypesEnum fiType);
	List<CustomerAccountTable> findByUniqueIdentifierAndLinkStatusAndFiTypeIn(String uniqueIdentifier, AccountLinkStatus linkStatus, List<FiTypesEnum> fiTypes);
	List<CustomerAccountTable> findByUniqueIdentifierAndLastFetchTimeLessThanAndLinkStatus(String uniqueIdentifier, Date lastFetchTime, AccountLinkStatus linkStatus);
	List<CustomerAccountTable> findByUniqueIdentifier(String uniqueIdentifier);

	@Query(nativeQuery = true, value = "Select * FROM fiu_pfm_customer_accounts WHERE ((fiu_customer_id = :uniqueIdentifier and account_id > :accountId) or  (fiu_customer_id > :uniqueIdentifier )) and update_time >= :fromDate and update_time < :toDate and encrypted = :encrypted LIMIT :limit")
	List<CustomerAccountTable> getEncryptDecryptAccounts(@Param("uniqueIdentifier") String uniqueIdentifier, @Param("accountId") String accountId, @Param("fromDate") Date fromDate, @Param("toDate") Date toDate,  @Param("encrypted") char encrypted, @Param("limit") long limit);
	
	List<CustomerAccountTable> findByUniqueIdentifierAndAccountIdIn(String uniqueIdentifier, Collection<String> accountIds);
	
	@Query(nativeQuery = true, value = "Select * FROM fiu_pfm_customer_accounts WHERE (fiu_customer_id = :uniqueIdentifier and account_id > :accountId or  (fiu_customer_id > :uniqueIdentifier )) and last_fetch_time < :lastFetchTime and link_status = :#{#accountLinkStatus.toString()} LIMIT :limit")
	List<CustomerAccountTable> findByCustomerIdGreaterThanEqualToAndAccountIdGreaterThanAndLastFetchTimeLessThanAndLinkStatus(@Param("uniqueIdentifier") String uniqueIdentifier, @Param("accountId") String accountId, @Param("lastFetchTime") Date lastFetchTime, @Param("accountLinkStatus") AccountLinkStatus accountLinkStatus,@Param("limit") long limit);
	
	@Modifying
	@Transactional
	@Query(nativeQuery = true, value = "DELETE FROM fiu_pfm_customer_accounts WHERE fiu_customer_id = :uniqueIdentifier")
	int deleteByUniqueIdentifier(String uniqueIdentifier);
	
	@Modifying
	@Transactional
	@Query("update CustomerAccountTable cat set cat.linkStatus = :linkStatus, cat.linkStatusUpdateTime = :linkStatusUpdateTime, cat.updateTime = :updateTime where cat.uniqueIdentifier = :uniqueIdentifier and cat.accountId = :accountId")
	int setAccountLinkStatusForAccountIdAndUniqueIdentifier(@Param("linkStatus") AccountLinkStatus linkStatus, @Param("linkStatusUpdateTime") Date linkStatusUpdateTime, @Param("updateTime") Date updateTime, @Param("uniqueIdentifier") String uniqueIdentifier, @Param("accountId") String accountId);

	@Query(nativeQuery = true, value = "select count(*) from fiu_pfm_customer_accounts c where c.linked_date < ?1 ")
	long countTotalNoOfAccounts(final Date endTime);

	@Query(nativeQuery = true, value = "select count(*) from fiu_pfm_customer_accounts c where c.linked_date >= ?1 and c.linked_date < ?2")
	long countByLinkedDateGreaterThanEqualAndLinkedDateLessThan(Date startTime, Date endTime);

	@Query(nativeQuery = true, value = "select count(*) from fiu_pfm_customer_accounts c where c.link_status = ?1 and c.linked_date <2")
	long countByLinkStatusEquals(final String linkStatus, final Date endTime);

	@Query(nativeQuery = true, value = "select count(*) from fiu_pfm_customer_accounts c where c.link_status = ?1 and c.linked_date >= ?2 and c.linked_date < ?3")
	long countByLinkStatusEqualsAndLinkedDateGreaterThanEqualAndLinkedDateLessThan(String linkStatus, Date startTime, Date endTime);

	@Query(nativeQuery = true, value = "select fip_id, count(distinct account_id) from fiu_pfm_customer_accounts where link_status = :linkStatus and linked_date >= :startTime and linked_date < :endTime group by fip_id")
	List<Object[]> findCountOfDistinctAccountIdGroupByFipId(String linkStatus, Date startTime, Date endTime);
	
	@Modifying
	@Transactional
	@Query("update CustomerAccountTable cat set cat.encrypted = :encrypted, cat.accountProfile = :accountProfile where cat.uniqueIdentifier = :uniqueIdentifier and cat.accountId = :accountId")
	int encryptDecryptAccountProfile(@Param("encrypted") char encrypted, @Param("accountProfile") String accountProfile, @Param("uniqueIdentifier") String uniqueIdentifier, @Param("accountId") String accountId);



	@Query(nativeQuery = true, value = "select count(distinct fiu_customer_id) from fiu_pfm_customer_accounts c where c.linked_date < ?1")
	long totalUniqueUsers(final Date endTime);

	@Query(nativeQuery = true, value = "select count(distinct fiu_customer_id) from fiu_pfm_customer_accounts c where c.linked_date >= ?1 and c.linked_date < ?2")
	long totalUniqueUsersInDateRange(final Date startDate, final Date EndDate);

	@Query(nativeQuery = true, value = "select count(distinct c.fiu_customer_id) from fiu_pfm_customer_accounts c where c.fi_type = ?1 and  c.linked_date < ?2")
	long totalUniqueUsersPerFiType(final String fiType, final Date endTime);

	@Query(nativeQuery = true, value = "select count(distinct fiu_customer_id) from fiu_pfm_customer_accounts c where c.fi_type = ?1 and c.linked_date >= ?2 and c.linked_date < ?3")
	long totalUniqueUsersPerFiTypeInDateRange(final String fiType, final Date startDate, final Date endDate);

}
