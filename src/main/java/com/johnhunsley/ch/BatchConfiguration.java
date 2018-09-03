package com.johnhunsley.ch;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.transform.*;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;

import java.util.ArrayList;
import java.util.IllegalFormatException;
import java.util.Iterator;
import java.util.List;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Value("${aws.accessKeyId}")
    private String awsAccessKey;

    @Value("${aws.secretKey}")
    private String awsSecretKey;

    @Value("${s3.bucket.name}")
    private String bucketName;

    @Value("${s3.key.name}")
    private String objectKey;

    @Value("${json.output.file.path}")
    private String jsonOutputFilePath;

    @Value("${csv.output.file.path}")
    private String csvOutputFilePath;

    @Autowired
    private AmazonS3 amazonS3;

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Bean
    public ObjectMapper mapper() {
        return new ObjectMapper();
    }



    @Bean
    public FlatFileItemReader<Company> reader() {
        return new FlatFileItemReaderBuilder<Company>()
                .name("messageReader")
                .resource(getS3ObjectAsResource())
                .lineTokenizer(new DelimitedLineTokenizer() {{
                    setIncludedFields(new int[] {0,1});
                    setNames("CompanyName", "CompanyNumber");
                }})
                .fieldSetMapper(new BeanWrapperFieldSetMapper<Company>() {{
                    setTargetType(Company.class);
                }})
                .build();
    }

    private Resource getS3ObjectAsResource() {
        S3Object s3object = amazonS3.getObject(new GetObjectRequest(bucketName, objectKey));
        return new InputStreamResource(s3object.getObjectContent());
    }

    @Bean
    public AmazonS3 buildClient() {

        if(awsAccessKey == null || awsSecretKey == null) {
            return AmazonS3ClientBuilder
                .standard()
                .withCredentials(InstanceProfileCredentialsProvider.getInstance())
                .withRegion(Regions.EU_WEST_1)
                .build();

        } else {
            AWSCredentials credentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
            return AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .withRegion(Regions.EU_WEST_1)
                    .build();
        }
    }

    @Bean
    public CompositeItemWriter<Company> compositeItemWriter() {
        CompositeItemWriter<Company> writer = new CompositeItemWriter<>();
        List<ItemWriter<? super Company>> writers = new ArrayList<>();
        writers.add(jsonFileItemWriter());
        writers.add(csvFileItemWriter());
        writer.setDelegates(writers);
        return writer;
    }

    private FlatFileItemWriter<Company> jsonFileItemWriter() {
        FlatFileItemWriter<Company> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource(jsonOutputFilePath));
        writer.setLineAggregator(new DelimitedLineAggregator<Company>() {
            @Override
            public String aggregate(Company company) {
                try {
                    return mapper().writeValueAsString(company);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                    throw new IllegalArgumentException(e);
                }
            }

            @Override
            public void setDelimiter(String delimiter) {
                super.setDelimiter("\\n");
            }
        });

        return writer;
    }

    private FlatFileItemWriter<Company> csvFileItemWriter() {
        FlatFileItemWriter<Company> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource(csvOutputFilePath));
        DelimitedLineAggregator<Company> aggregator = new DelimitedLineAggregator<>();
        aggregator.setDelimiter(",");
        BeanWrapperFieldExtractor extractor = new BeanWrapperFieldExtractor();
        extractor.setNames(new String[] { "companyNumber", "companyName"});
        aggregator.setFieldExtractor(extractor);
        writer.setLineAggregator(aggregator);
        return writer;
    }



    @Bean
    public Step step1(ItemWriter writer) {
        return stepBuilderFactory.get("step1").<Company, Company> chunk(100)
                .reader(reader())
                .writer(writer)
                .build();
    }

    @Bean
    public Job importFromS3AndWriteJob(Step step1) {
        return jobBuilderFactory.get("importS3Job")
                .listener(new JobExecutionListenerSupport() {
                    @Override
                    public void afterJob(JobExecution jobExecution) {
                        if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
                            System.out.println("Job finished. do something meaningful like notify someone.");
                        }
                    }
                })
                .flow(step1)
                .end()
                .build();
    }

}
