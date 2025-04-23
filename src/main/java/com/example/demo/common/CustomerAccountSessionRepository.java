package com.example.demo.common;

import com.ftpl.pfm.common.model.session.CustomerAccountSessionId;
import com.ftpl.pfm.common.model.session.CustomerAccountSessionTable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface CustomerAccountSessionRepository extends JpaRepository<CustomerAccountSessionTable, CustomerAccountSessionId> {
    List<CustomerAccountSessionTable> findByUniqueIdentifierAndSessionId(String uniqueIdentifier, String sessionId);

    
}
