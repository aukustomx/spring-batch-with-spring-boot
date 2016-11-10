package io.augusto.example.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    private static final String QUERY_FIND_PEOPLE = "SELECT first_name, last_name FROM people";

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public DataSource dataSource;

    // tag::readerwriterprocessor[]
    @Bean
    public FlatFileItemReader<Person> csvReader() {
        FlatFileItemReader<Person> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("sample-data.csv"));
        reader.setLineMapper(new DefaultLineMapper<Person>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[] { "firstName", "lastName" });
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                setTargetType(Person.class);
            }});
        }});
        return reader;
    }

    @Bean
    public PersonToUppercaseItemProcessor processorToUppercase() {
        return new PersonToUppercaseItemProcessor();

    }

    @Bean
    public JdbcBatchItemWriter<Person> writer() {
        JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<>();
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        writer.setSql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)");
        writer.setDataSource(dataSource);
        return writer;
    }
    // end::readerwriterprocessor[]

    // tag::jobstep[]
    @Bean
    public Job importUserJob(JobCompletionNotificationListener listener) {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(stepToUppercase())
                .next(stepToLowercase())
                .end()
                .build();
    }

    @Bean
    public Step stepToUppercase() {
        return stepBuilderFactory.get("stepToUppercase")
                .<Person, Person> chunk(10)
                .reader(csvReader())
                .processor(processorToUppercase())
                .writer(writer())
                .build();
    }
    // end::jobstep[]

    //star::Step2 and its elements
    @Bean
    public ItemReader<Person> jdbcReader(DataSource dataSource) {

        JdbcCursorItemReader<Person> reader = new JdbcCursorItemReader<>();

        reader.setDataSource(dataSource);
        reader.setSql(QUERY_FIND_PEOPLE);
        reader.setRowMapper(new BeanPropertyRowMapper<>(Person.class));

        return reader;
    }

    @Bean
    public PersonToLowercaseItemProcessor processorToLowercase() {
        return new PersonToLowercaseItemProcessor();
    }

    @Bean
    public Step stepToLowercase() {
        return stepBuilderFactory.get("step2")
                .<Person, Person>chunk(10)
                .reader(jdbcReader(dataSource))
                .processor(processorToLowercase())
                .writer(writer())
                .build();
    }


    //end::Step2 and its elements
}
