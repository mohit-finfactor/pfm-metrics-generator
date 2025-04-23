package com.example.demo.common;

import com.ftpl.pfm.common.enums.ConsentStatus;
import com.ftpl.pfm.common.model.consent.CustomerConsentId;
import com.ftpl.pfm.common.model.consent.CustomerConsentTable;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Date;
import java.util.List;

public interface CustomerConsentRepository extends JpaRepository<CustomerConsentTable, CustomerConsentId> {
	List<CustomerConsentTable> findByUniqueIdentifier(String uniqueIdentifier);
	List<CustomerConsentTable> findByConsentHandleId(String consentHandleId);
	List<CustomerConsentTable> findByUniqueIdentifierAndAccountIdAndConsentStatus(String uniqueIdentifier, String accountId, ConsentStatus consentStatus);
	List<CustomerConsentTable> findByUniqueIdentifierAndConsentStatus(String uniqueIdentifier, ConsentStatus consentStatus);
	List<CustomerConsentTable> findByUniqueIdentifierAndConsentHandleId(String uniqueIdentifier, String consentHandleId);
	List<CustomerConsentTable> findByUniqueIdentifierAndConsentHandleIdAndConsentId(String uniqueIdentifier, String consentHandleId, String consentId);
	List<CustomerConsentTable> findByUniqueIdentifierAndConsentId(String uniqueIdentifier, String consentId);
	
	List<CustomerConsentTable> findByUniqueIdentifierAndAccountIdAndConsentStatusOrderByConsentCreateTimeDesc(String uniqueIdentifier, String accountId, ConsentStatus consentStatus);
	
	@Modifying
	@Transactional
	@Query(nativeQuery = true, value = "DELETE FROM fiu_pfm_customer_consents WHERE fiu_customer_id = :uniqueIdentifier")
	int deleteByUniqueIdentifier(String uniqueIdentifier);
	
	@Modifying
	@Transactional
	@Query("update CustomerConsentTable cct set cct.consentStatus = :consentStatus, cct.consentStatusUpdateTime = :consentStatusUpdateTime where cct.consentHandleId = :consentHandleId")
	int setConsentStatusForConsentHandleId(@Param("consentStatus") ConsentStatus consentStatus, @Param("consentStatusUpdateTime") Date consentStatusUpdateTime, @Param("consentHandleId") String consentHandleId);

	@Query(nativeQuery = true, value = "select count(distinct fiu_customer_id) from fiu_pfm_customer_consents c where c.consent_status = ?1")
	long countDistinctUniqueIdentifierByConsentStatusEquals(String consentStatus);

	@Query(nativeQuery = true, value = "select count(distinct c.fiu_customer_id) from fiu_pfm_customer_consents c inner join fiu_pfm_customer_consents_handle h on h.consent_handle_id = c.consent_handle_id where c.consent_status = ?1 and h.consent_create_time >= ?2 and h.consent_create_time < ?3")
	long countByDistinctUniqueIdentifierByConsentStatusEqualsAndCreateTimeGreaterThanEqualAndCreateTimeLessThan(String status, Date startTime, Date endTime);

	@Query(nativeQuery = true, value = "select count(*) from fiu_pfm_customer_consents c where c.consent_status = ?1 and c.consent_status_update_time < ?2")
	long countByConsentStatusEquals(final String consentStatus, final Date endTime);

	@Query(nativeQuery = true, value = "select count(*) from fiu_pfm_customer_consents c inner join fiu_pfm_customer_consents_handle h on h.consent_handle_id = c.consent_handle_id where c.consent_status = ?1 and h.consent_create_time >= ?2 and h.consent_create_time < ?3")
	long countByConsentStatusEqualsAndCreateTimeGreaterThanEqualAndCreateTimeLessThan(String status, Date startTime, Date endTime);
	
	@Query(nativeQuery = true, value = "select count(*) from fiu_pfm_customer_consents c where c.consent_status = ?1 and c.consent_status_update_time >= ?2 and c.consent_status_update_time < ?3")
	long countByConsentStatusEqualsAndUpdateTimeGreaterThanEqualAndUpdateTimeLessThan(String status, Date startTime, Date endTime);

	@Query("SELECT c FROM CustomerConsentTable c " +
			"INNER JOIN ( " +
				"SELECT c2.accountId as aid, MAX(c2.consentExpiryTime) as maxExpiry " +
					"FROM CustomerConsentTable c2 " +
					"WHERE c2.uniqueIdentifier = :uniqueIdentifier " +
						"AND c2.accountId IN :accountIds " +
						"AND c2.consentStatus = :consentStatus " +
					"GROUP BY c2.accountId) " +
			"c2 ON c2.aid = c.accountId " +
			"AND c.consentExpiryTime = c2.maxExpiry")
	List<CustomerConsentTable> findLatestConsentsForAccountIdsAndUniqueIdentifier(
			@Param("uniqueIdentifier") final String uniqueIdentifier,
			@Param("accountIds") final List<String> accountIds,
			@Param("consentStatus") final ConsentStatus consentStatus
	);
}
