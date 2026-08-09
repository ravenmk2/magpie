package ravenworks.magpie.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ravenworks.magpie.domain.entity.TargetEntity;


public interface TargetRepository extends JpaRepository<TargetEntity, String> {

}
