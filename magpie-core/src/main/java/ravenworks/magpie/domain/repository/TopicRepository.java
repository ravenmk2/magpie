package ravenworks.magpie.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ravenworks.magpie.domain.entity.TopicEntity;


public interface TopicRepository extends JpaRepository<TopicEntity, String> {

}
