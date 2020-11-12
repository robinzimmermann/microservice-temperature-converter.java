/**
 * Copyright 2016-2020 Tenefit. All rights reserved.
 */
package com.tenefit.demo.microservice.temperatureConverter;

import static com.tenefit.demo.microservice.temperatureConverter.TemperatureConverter.Protocol.PLAINTEXT;
import static com.tenefit.demo.microservice.temperatureConverter.TemperatureConverter.Protocol.SSL;
import static java.lang.System.currentTimeMillis;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.AllowedEnumValues;
import com.github.rvesse.airline.annotations.restrictions.NotBlank;
import com.github.rvesse.airline.annotations.restrictions.NotEmpty;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.github.rvesse.airline.help.Help;
import com.github.rvesse.airline.parser.errors.ParseException;

@Command(name = "temperature-converter", description = "Microservice for converting temperatures")
public class TemperatureConverter
{
    public enum Protocol
    {
        PLAINTEXT,
        SSL
    };

    private final String defaultGroupId = String.format("temperature-converter-%x", currentTimeMillis());

    private final Duration kafkaPollTimeout = Duration.ofSeconds(1000);

    @Inject
    protected HelpOption<TemperatureConverter> help;

    @Option(
        name = { "--bootstrap-servers", "-b" },
        description = "Address for Kafka. e.g. kafka:9092\n" +
            "Ports 9093 and 9094 default to protocol SSL. All others default to PLAINTEXT")
    @Required
    @Once
    @NotBlank
    @NotEmpty
    private String kafkaAddress;

    @Option(
        name = { "--input-topic", "-i" },
        description = "Input topic with raw sensor readings. Defaults to \"sensors\"")
    @Once
    @NotBlank
    @NotEmpty
    private String inputTopic = "sensors";

    @Option(
        name = { "--output-topic", "-o" },
        description = "Output topic for converted readings. Defaults to \"readings\"")
    @Once
    @NotBlank
    @NotEmpty
    private String outputTopic = "readings";

    @Option(
        name = { "--requests-topic", "-r" },
        description = "Input topic for microservice command requests. Defaults to \"readings.requests\"")
    @Once
    @NotBlank
    @NotEmpty
    private String requestsTopic = "readings.requests";

    @Option(
        name = { "--protocol", "-p" },
        description = "Type of connection to make")
    @AllowedEnumValues(Protocol.class)
    @Once
    @NotBlank
    @NotEmpty
    private Protocol protocol;

    @Option(
        name = "--consumer-property",
        description = "Kafka consumer property. May be repeated. Format: <key>=<value>. e.g. session.timeout.ms=5000")
    @NotBlank
    @NotEmpty
    private List<String> consumerPropertiesArgs;

    @Option(
        name = "--producer-property",
        description = "Kafka producer property. May be repeated. Format: <key>=<value>. e.g. batch.size=16384")
    @NotBlank
    @NotEmpty
    private List<String> producerPropertiesArgs;

    @Option(
        name = { "--verbose", "-v" },
        description = "Show verbose output at start up")
    @Once
    private boolean verbose;

    private final SortedMap<String, Object> consumerOptions;
    private final SortedMap<String, Object> producerOptions;

    private volatile boolean isRunning = true;

    public static void main(String[] args) throws Exception
    {
        SingleCommand<TemperatureConverter> parser = SingleCommand.singleCommand(TemperatureConverter.class);
        TemperatureConverter microservice = parser.parse(args);
        try
        {
            microservice.start();
        }
        catch (ParseException e)
        {
            System.err.format("Error: %s\n\n", e.getMessage());
            Help.help(parser.getCommandMetadata());
        }

    }

    public TemperatureConverter() throws Exception
    {
        consumerOptions = new TreeMap<>();
        consumerOptions.put("group.id", defaultGroupId);

        producerOptions = new TreeMap<>();
    }

    public void start() throws InterruptedException, ExecutionException
    {
        if (help.showHelpIfRequested())
        {
            return;
        }

        processCommandLine();

        final Thread myThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            public void run()
            {
                isRunning = false;
                myThread.interrupt();
                try
                {
                    myThread.join();
                }
                catch (InterruptedException ex)
                {
                    ex.printStackTrace();
                }
            }
        });

        try
        {
            startListening();
        }
        catch (InterruptException ex)
        {
            // User pressed Ctrl-C while Kafka library was blocking. Do nothing.
        }
        catch (KafkaException ex)
        {
            // Most likely an invalid Kafka address.
            System.out.format("ERROR: %s\n", ex);
            if (ex.getCause() != null)
            {
                System.out.format("Cause: %s\n", ex.getCause().getMessage());
            }
        }
    }

    private void processCommandLine()
    {
        if (protocol == null)
        {
            String[] addressParts = kafkaAddress.split(":");
            if (addressParts.length != 2)
            {
                throw new ParseException("The Kafka address %s is not formatted as <host>:<port>", kafkaAddress);
            }
            if (addressParts[1].equals("9093") || addressParts[1].equals("9094"))
            {
                protocol = SSL;
            }
            else
            {
                protocol = PLAINTEXT;
            }
        }

        consumerOptions.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        consumerOptions.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerOptions.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        producerOptions.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        producerOptions.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerOptions.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        if (protocol == SSL)
        {
            consumerOptions.put("security.protocol", "SSL");
            consumerOptions.put("ssl.endpoint.identification.algorithm", "");
            producerOptions.put("security.protocol", "SSL");
            producerOptions.put("ssl.endpoint.identification.algorithm", "");
        }

        // Do command line arguments last so they can override default behavior
        if (consumerPropertiesArgs != null)
        {
            consumerPropertiesArgs.forEach(this::parseKafkaConsumerProperty);
        }
        if (producerPropertiesArgs != null)
        {
            producerPropertiesArgs.forEach(this::parseKafkaProducerProperty);
        }
    }

    private void parseKafkaConsumerProperty(
        String arg)
    {
        String[] pair = arg.split("=");
        consumerOptions.put(pair[0], pair.length == 2 ? pair[1] : "");
    }

    private void parseKafkaProducerProperty(
        String arg)
    {
        String[] pair = arg.split("=");
        producerOptions.put(pair[0], pair.length == 2 ? pair[1] : "");
    }

    private void startListening() throws InterruptedException, ExecutionException
    {
        if (verbose)
        {
            System.out.format("kafka address:          %s\n", kafkaAddress);
            System.out.format("input topic:            %s\n", inputTopic);
            System.out.format("output topic:           %s\n", outputTopic);
            System.out.format("readings request topic: %s\n", requestsTopic);
            System.out.println("consumer properties:");
            for (Map.Entry<String, Object> prop : consumerOptions.entrySet())
            {
                System.out.format("  %s=%s\n", prop.getKey(), prop.getValue());
            }
            System.out.println("producer properties:");
            for (Entry<String, Object> prop : producerOptions.entrySet())
            {
                System.out.format("  %s=%s\n", prop.getKey(), prop.getValue());
            }
        }

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerOptions))
        {
            consumer.subscribe(Arrays.asList(inputTopic, requestsTopic));

            SensorsMessageHandler sensorsMessageHandler = new SensorsMessageHandler(
                new KafkaProducer<String, String>(producerOptions),
                outputTopic);

            ReadingsMessageHandler readingsMessageHandler = new ReadingsMessageHandler(
                new KafkaProducer<String, String>(producerOptions));

            TemperatureUnit readingsUnit = TemperatureUnit.F;

            System.out.println("TemperatureConverter microservice listening");

            while (isRunning)
            {
                ConsumerRecords<String, String> records = consumer.poll(kafkaPollTimeout);
                for (ConsumerRecord<String, String> record: records)
                {
                    if (record.topic().equals(inputTopic))
                    {
                        sensorsMessageHandler.handleMessage(record, readingsUnit);
                    }
                    else
                    {
                        final TemperatureUnit newReadingsUnit = readingsMessageHandler.handleMessage(record);
                        if (newReadingsUnit != null)
                        {
                            readingsUnit = newReadingsUnit;
                        }
                    }
                }
            }
        }
    }
}
