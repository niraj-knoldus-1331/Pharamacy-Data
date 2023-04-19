package com.knoldus.pharmacy.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface PubSubOptions extends PipelineOptions {

    @Description("The Cloud Pub/Sub topic to read from.")
    @Validation.Required
    @Default.String("projects/de-da-ml/topics/pharmacy-medication-form-data")
    String getInputTopic();

    void setInputTopic(String value);

    @Description("The cloud Pub/Sub subscription to read from")
    @Validation.Required
    @Default.String("projects/de-da-ml/subscriptions/pharmacy-medication-form-data_beam_-5688491694422031163")
    String getInputSubscription();

    void setInputSubscription(String inputSubscription);

    @Description("Whether to use topic or Pub/Sub")
    @Validation.Required
    @Default.Boolean(true)
    Boolean getUseSubscription();

    void setUseSubscription(Boolean useSubscription);


}
