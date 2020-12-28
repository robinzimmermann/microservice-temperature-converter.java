/**
 * Copyright 2016-2020 Tenefit. All rights reserved.
 */
package com.tenefit.demo.microservice.temperatureConverter;

import java.io.StringReader;
import java.util.concurrent.ExecutionException;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.stream.JsonParsingException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

public class SensorsMessageHandler
{
    private final Producer<String, String> producer;
    private final String outputTopic;

    public SensorsMessageHandler(
        final Producer<String, String> producer,
        String outputTopic)
    {
        this.producer = producer;
        this.outputTopic = outputTopic;
    }

    public void handleMessage(
        ConsumerRecord<String, String> input,
        TemperatureUnit outputUnit) throws InterruptedException, ExecutionException
    {
        String inputKey = input.key();
        Header inputRow = input.headers().lastHeader("row");

        /*
        ProducerRecord<String, String> output = new ProducerRecord<>(
            outputTopic, inputKey, String.format("{\"id\":\"%s\",\"value\":%d,\"unit\":\"F\"}",
                inputKey, ThreadLocalRandom.current().nextInt(75, 200 + 1)));
        output.headers().add(inputRow);
        producer.send(output).get();
        */

        try (JsonReader inputJson = Json.createReader(new StringReader(input.value())))
        {
            JsonObject inputReading = inputJson.readObject();
            if (inputRow != null && inputReading.containsKey("value") && inputReading.containsKey("unit"))
            {
                int inputValue = inputReading.getInt("value");
                TemperatureUnit inputUnit = TemperatureUnit.valueOf(inputReading.getString("unit"));

                String outputReading = Json.createObjectBuilder()
                    .add("id", inputKey)
                    .add("value", outputUnit.canonicalize(inputValue, inputUnit))
                    .add("unit", outputUnit.toString())
                    .build()
                    .toString();

                ProducerRecord<String, String> output = new ProducerRecord<>(outputTopic, inputKey, outputReading);
                output.headers().add(inputRow);
                producer.send(output).get();
            }
        }
        catch (JsonParsingException ex)
        {
            // The payload on the input message was not valid JSON, so skip it
        }
    }
}
