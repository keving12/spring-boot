package com.kg.driver;

import com.kg.config.Configuration;
import com.kg.elastic.ScrollClient;
import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by kevingracie on 22/09/2016.
 */
@SpringBootApplication
public class Application {

    private static final Logger LOGGER = Logger.getLogger(Application.class);

    public static void main(String[] args) {
        if(args.length < 1) {
            LOGGER.error("Did not meet minimum requirements for number of arguments");
            throw new IllegalStateException("Required arguments missing - (configuration file)");
        }

        ApplicationContext ctx = SpringApplication.run(Application.class, args);

        final Yaml yaml = new Yaml();
        final Configuration config;
        try(InputStream configStream = Files.newInputStream(Paths.get(args[0]))) {
            config = yaml.loadAs(configStream, Configuration.class);
            LOGGER.debug("Config loaded successfully");

        } catch(IOException e) {
            LOGGER.error("Could not load configuration from file.", e);
            throw new IllegalStateException("Could not load configuration from file specified");
        }

        final ScrollClient client = new ScrollClient(config);
//        client.moveAllDocuments();
//        client.deleteDocumentsOfType("data_v2");
        client.getMetadata();
        SpringApplication.exit(ctx);

    }
}
