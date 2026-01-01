## Payroll Processing App
This app is used for uploading payroll and user data through csv files, and process it through SQL procedures, following by validating the processed payroll in the PowerApp using metrics.

1. User uploads the files:
    <img width="1335" height="737" alt="image" src="https://github.com/user-attachments/assets/61ac402b-481f-4200-97c2-71a6fcaeec8b" />
    In the background: The files get uploaded to ADLS Gen2 using PowerAutomate flows, and is processing using Function App and a Databricks Spark notebook, orchestrated using Azure Data Factory pipeline.

2. User chooses a validated Payroll Start and End Date:
    <img width="1337" height="738" alt="image" src="https://github.com/user-attachments/assets/cd40da5b-8b3f-411f-877b-9b6fe8a3bd41" />
    In the background: A SQL Stored Procedure gets triggered, which will process the uploaded data and generate a payroll table.
3. A report page displays the processed payroll metrics for validation
     <img width="1337" height="730" alt="image" src="https://github.com/user-attachments/assets/05689383-4d3d-4723-9d2a-64a5e47a3f8c" />

5. Finally, a data table allows the user to review and download the generated payroll data.
     <img width="1347" height="742" alt="image" src="https://github.com/user-attachments/assets/8fbf6660-0305-4381-a8f5-49422b1607c1" />

