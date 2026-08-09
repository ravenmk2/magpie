package ravenworks.magpie.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ravenworks.magpie.domain.entity.MessageLogEntity;


public interface MessageLogRepository extends JpaRepository<MessageLogEntity, String> {

}
