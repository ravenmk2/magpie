package ravenworks.magpie.server.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverters;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import ravenworks.magpie.server.web.CloudEventHttpMessageConverter;


/**
 * @author Raven
 */
@Configuration
public class CloudEventWebMvcConfigurer implements WebMvcConfigurer {

    @Override
    public void configureMessageConverters(HttpMessageConverters.ServerBuilder builder) {
        builder.addCustomConverter(new CloudEventHttpMessageConverter());
    }

}
