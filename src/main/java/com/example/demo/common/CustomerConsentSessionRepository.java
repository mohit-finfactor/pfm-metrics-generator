package com.example.demo.common;

import com.ftpl.pfm.common.enums.ConsentSessionStatus;
import com.ftpl.pfm.common.model.consent.CustomerConsentSessionTable;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.util.Date;
import java.util.List;

public interface CustomerConsentSessionRepository extends JpaRepository<CustomerConsentSessionTable, String> {
	List<CustomerConsentSessionTable> findByUniqueIdentifier(String uniqueIdentifier);
	List<CustomerConsentSessionTable> findByUniqueIdentifierAndSessionFiDataRangeToIsGreaterThanEqualOrderBySessionFiDataRangeTo(String uniqueIdentifier, Date sessionFiDataRangeTo);
	List<CustomerConsentSessionTable> findBySessionId(String sessionId);
	List<CustomerConsentSessionTable> findByUniqueIdentifierAndConsentIdAndSessionFiDataRangeToIsGreaterThanEqualOrderBySessionFiDataRangeTo(String uniqueIdentifier, String consentId, Date sessionFiDataRangeTo);
	List<CustomerConsentSessionTable> findBySessionStatusAndSessionFiDataRangeToIsGreaterThanEqual(ConsentSessionStatus consentSessionStatus, Date sessionFiDataRangeTo);
	CustomerConsentSessionTable findByConsentId(String consentId);
	@Modifying
	@Transactional
	@Query(nativeQuery = true, value = "DELETE FROM fiu_pfm_customer_consent_session WHERE fiu_customer_id = :uniqueIdentifier")
	int deleteByUniqueIdentifier(String uniqueIdentifier);

	@Query(nativeQuery = true, value = "select IFNULL(a.fip_id, 'UNKNOWN'), COUNT(distinct c.account_id) from  fiu_pfm_customer_consent_session as s " +
			"left join fiu_pfm_customer_consents as c on c.consent_id = s.consent_id " +
			"left join fiu_pfm_customer_accounts as a on a.account_id = c.account_id " +
			"where s.session_fidata_range_to >= :startDate and s.session_fidata_range_to < :endDate group by a.fip_id")
	List<Object[]> findCountOfAccountIdGroupByFIP(Date startDate, Date endDate);

	@Query(nativeQuery = true, value = "select IFNULL(a.fip_id, 'UNKNOWN'), COUNT(distinct c.account_id) " +
			"from  fiu_pfm_customer_consent_session as s " +
			"left join fiu_pfm_customer_consents as c on c.consent_id = s.consent_id " +
			"left join fiu_pfm_customer_accounts as a on a.account_id = c.account_id " +
			"where s.session_fidata_range_to >= :startDate and s.session_fidata_range_to < :endDate and s.session_status = :status group by a.fip_id")
	List<Object[]> findBySessionStatusCountOfAccountIdGroupByFIP(String status, Date startDate, Date endDate);
}
