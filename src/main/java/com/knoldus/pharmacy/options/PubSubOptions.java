package com.knoldus.pharmacy.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface PubSubOptions extends PipelineOptions {

    @Description("The Cloud Pub/Sub topic to read from.")
    @Validation.Required
    @Default.String("projects/de-da-poc/topics/pharmacy-medication-form-data")
    String getInputTopic();

    void setInputTopic(String value);

    @Description("The cloud Pub/Sub subscription to read from")
    @Validation.Required
    @Default.String("projects/de-da-poc/subscriptions/pharmacy-medication-form-data-sub")
    String getInputSubscription();

    void setInputSubscription(String inputSubscription);

    @Description("Whether to use topic or Pub/Sub")
    @Validation.Required
    @Default.Boolean(true)
    Boolean getUseSubscription();

    void setUseSubscription(Boolean useSubscription);


}
