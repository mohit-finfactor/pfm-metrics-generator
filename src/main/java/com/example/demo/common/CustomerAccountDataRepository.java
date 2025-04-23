package com.example.demo.common;

import com.ftpl.pfm.common.model.deposit.CustomerAccountDataTable;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Date;
import java.util.List;

public interface CustomerAccountDataRepository  extends JpaRepository<CustomerAccountDataTable, String> {
	
	List<CustomerAccountDataTable> findByUniqueIdentifierAndAccountIdAndTxnDateBetweenOrderByTxnDateAsc(String uniqueIdentifier, String accountId, Date dateRangeFrom, Date dateRangeTo );

	List<CustomerAccountDataTable> findByUniqueIdentifierAndAccountId(String uniqueIdentifier, String accountId);
	List<CustomerAccountDataTable> findByUniqueIdentifier(String uniqueIdentifier);

	@Query(nativeQuery = true, value ="Select * from fiu_pfm_customer_account_data where txn_id > :txn_id and update_time >= :fromDate and update_time < :toDate and encrypted = :encrypted LIMIT :limit")
	List<CustomerAccountDataTable> getEncryptDecryptAccountDatas(@Param("txn_id") String txn_id, @Param("fromDate") Date fromDate, @Param("toDate") Date toDate,  @Param("encrypted") char encrypted, @Param("limit") long limit);


//	@Modifying
//	@Transactional
//	@Query(nativeQuery = true, value = "insert into fiu_pfm_customer_account_data (account_transaction, txn_date, account_id, txn_id, fiu_customer_id, update_time, transaction_classification) values (:#{#cadt.accountTransaction}, :#{#cadt.txnDate}, :#{#cadt.accountId}, :#{#cadt.txnId}, :#{#cadt.uniqueIdentifier}, :#{#cadt.updateTime}, :#{#cadt.transaction_classification})")
//	void insertAccountData(@Param("cadt") CustomerAccountDataTable customerAccountDataTable);

	@Modifying
	@Transactional
	@Query(nativeQuery = true, value = "update fiu_pfm_customer_account_data set transaction_classification = :#{#cadt.transactionClassification}, update_time = :#{#cadt.updateTime} WHERE txn_id = :#{#cadt.txnId}")
	void updateAccountDataClassification(@Param("cadt") CustomerAccountDataTable customerAccountDataTable);
	
	@Transactional
	long deleteByUniqueIdentifierAndAccountIdAndTxnDateIsGreaterThanEqual(String uniqueIdentifier, String accountId, Date lastFetchTime);
	@Transactional
	long deleteByUniqueIdentifierAndAccountId(String uniqueIdentifier, String accountId);
	
	@Modifying
	@Transactional
	@Query(nativeQuery = true, value = "DELETE FROM fiu_pfm_customer_account_data WHERE fiu_customer_id = :uniqueIdentifier")
	int deleteByUniqueIdentifier(String uniqueIdentifier);
	
	@Modifying
	@Transactional
	@Query("update CustomerAccountDataTable cadt set cadt.encrypted = :encrypted, cadt.accountTransaction = :accountTransaction where cadt.txnId = :txnId")
	int encryptDecryptAccountData(@Param("encrypted") char encrypted, @Param("accountTransaction") String accountTransaction, @Param("txnId") String txnId);
	
}
