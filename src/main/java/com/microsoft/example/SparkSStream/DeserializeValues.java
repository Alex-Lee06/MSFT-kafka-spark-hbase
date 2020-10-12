package com.microsoft.example.SparkSStream;

public class DeserializeValues {
    private String kafkaValues;

    public DeserializeValues()
    {
    }

    public DeserializeValues(String kafkaValues){
        this.kafkaValues = kafkaValues;
    }

    public String getKafkaValues(){
        return this.kafkaValues;
    }

    @Override
    public String toString() {
        return kafkaValues;
    }
}
