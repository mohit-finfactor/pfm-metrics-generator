### 1. Prepare your JAR and Configuration File
   Place your compiled JAR file (e.g., pfm-metrics-generator-0.0.1.jar) in a folder.

In the same folder, create a file named exactly application.properties.

Put this content inside application.properties:
```.properties
# Database configuration
spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver
spring.datasource.url=jdbc:mysql://qa-db.cfga4w6mu6pl.ap-south-1.rds.amazonaws.com:3306/fiu_finsense_pfm_dhanaprayoga?useSSL=false&connectionTimeZone=UTC
spring.datasource.username=pfm_dhanaprayoga
spring.datasource.password=dhanaprayoga123

# Email server configuration
spring.mail.host=smtppro.zoho.in
spring.mail.port=587
spring.mail.username=support@finfactor.in
spring.mail.password=DellCase@2427
spring.mail.properties.mail.smtp.auth=true
spring.mail.properties.mail.smtp.starttls.enable=true
spring.mail.properties.mail.smtp.startssl.enable=true

# Email metadata
email.from=support@finfactor.in
email.to=mohitg@finfactor.in,harshs@finfactor.in
email.subject=PFM metrics - Axis - LIVE

# Job parameters
job.start-date=2024-05-01
job.end-date=2024-05-01
```

Now, your folder should have:
- pfm-metrics-generator-0.0.1.jar
- application.properties


### 2. Run your JAR using external application.properties
   Open your terminal/command prompt, navigate to that folder, and run:
```shell
java -jar pfm-metrics-generator-0.0.1.jar --spring.config.location=application.properties
```
