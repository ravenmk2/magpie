package ravenworks.magpie.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ravenworks.magpie.domain.entity.SourceEntity;


public interface SourceRepository extends JpaRepository<SourceEntity, String> {

}
