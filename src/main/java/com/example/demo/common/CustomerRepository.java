package com.example.demo.common;

import com.ftpl.pfm.common.model.customer.CustomerTable;
import com.ftpl.pfm.common.model.customer.SubscriptionStatusDao;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Date;
import java.util.List;

public interface CustomerRepository extends JpaRepository<CustomerTable, String> {
	List<CustomerTable> findByUniqueIdentifierAndMobileNumber(String uniqueIdentifier, String mobileNumber);
	List<CustomerTable> findByUniqueIdentifier(String uniqueIdentifier);
	List<CustomerTable> findByMobileNumber(String mobileNumber);

	@Query(nativeQuery = true, value ="Select * from fiu_pfm_customers where fiu_customer_id > :uniqueIdentifier and subscription_status = :#{#subscriptionStatus.toString()} LIMIT :limit")
	List<CustomerTable> findByUniqueIdentifierGreaterThanAndSubscriptionStatus(@Param("uniqueIdentifier") String uniqueIdentifier, @Param("subscriptionStatus") SubscriptionStatusDao subscriptionStatus, @Param("limit") long limit);
	
	@Query(nativeQuery = true, value ="Select * from fiu_pfm_customers where fiu_customer_id > :uniqueIdentifier LIMIT :limit")
	List<CustomerTable> findByUniqueIdentifierGreaterThan(@Param("uniqueIdentifier") String uniqueIdentifier, @Param("limit") long limit);
	
	@Modifying
	@Transactional
	@Query(nativeQuery = true, value = "DELETE FROM fiu_pfm_customers WHERE fiu_customer_id = :uniqueIdentifier")
	int deleteByUniqueIdentifier(String uniqueIdentifier);

	@Query(nativeQuery = true, value = "select count(*) from fiu_pfm_customers c where c.create_time < ?1")
	long countTotalNoOfCustomersRegistered(final Date endTime);

	@Query(nativeQuery = true, value = "select count(*) from fiu_pfm_customers c where c.create_time >= ?1 and c.create_time < ?2")
	long countByCreateTimeGreaterThanEqualAndCreateTimeLessThan(Date startTime, Date endTime);

	@Query(nativeQuery = true, value = "select count(*) from fiu_pfm_customers c where c.subscription_status = ?1 and c.update_time < ?2")
	long countBySubscriptionStatusEquals(final String subscriptionStatus, final Date endTime);

	@Query(nativeQuery = true, value = "select count(*) from fiu_pfm_customers c where c.subscription_status = ?1 and c.update_time >= ?2 and c.update_time < ?3")
	long countBySubscriptionStatusEqualsAndUpdateTimeGreaterThanEqualAndUpdateTimeLessThan(String subscriptionStatus, Date startTime, Date endTime);

	@Query(nativeQuery = true, value = "select count(*) from fiu_pfm_customers c where c.subscription_end >= ?1 and c.subscription_end < ?2")
	long countBySubscriptionEndGreaterThanEqualAndSubscriptionEndLessThan(Date subscriptionEnd, Date subscriptionEnd1);
}
