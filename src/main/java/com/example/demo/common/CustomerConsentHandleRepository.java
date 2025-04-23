package com.example.demo.common;

import com.ftpl.pfm.common.enums.ConsentHandleStatus;
import com.ftpl.pfm.common.model.consent.CustomerConsentHandleTable;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.util.Collection;
import java.util.Date;
import java.util.List;

public interface CustomerConsentHandleRepository extends JpaRepository<CustomerConsentHandleTable, String> {
	List<CustomerConsentHandleTable> findByUniqueIdentifier(String uniqueIdentifier);
	List<CustomerConsentHandleTable> findByConsentHandleId(String consentHandleId);
	List<CustomerConsentHandleTable> findByUniqueIdentifierAndConsentHandleStatus(String uniqueIdentifier, ConsentHandleStatus consentHandleStatus);
	List<CustomerConsentHandleTable> findByConsentHandleStatusAndConsentCreateTimeIsGreaterThanEqual(ConsentHandleStatus consentHandleStatus, Date consentCreateTime);
	
	@Modifying
	@Transactional
	@Query(nativeQuery = true, value = "DELETE FROM fiu_pfm_customer_consents_handle WHERE fiu_customer_id = :uniqueIdentifier")
	int deleteByUniqueIdentifier(String uniqueIdentifier);

	@Query(nativeQuery = true, value = "select count(*) from fiu_pfm_customer_consents_handle c where c.consent_fidata_range_to >= ?1 and c.consent_fidata_range_to < ?2")
	long countByConsentFiDataRangeToGreaterThanEqualAndConsentFiDataRangeToLessThan(Date startTime, Date endTime);

	List<CustomerConsentHandleTable> findByConsentHandleIdIn(Collection<String> consentHandleIds);
}
