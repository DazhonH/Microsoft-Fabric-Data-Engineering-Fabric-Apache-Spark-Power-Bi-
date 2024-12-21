# End-to-End Data Pipeline with Microsoft Fabric, Apache Spark, and Power BI

![Microsoft Fabric Data Pipeline](https://github.com/user-attachments/assets/403f9e09-0dfc-42e4-bf3b-d0b69a4f93ec)


## Project Overview
This project demonstrates a pipeline where data from the Bing News Search API is processed using Apache Spark and visualized using Power BI. The pipeline cleans, aggregates, and prepares the data for data visualization. The main purpose of this project was to gain hands on experience using Microsoft Fabric in order to pass the DP-600 which is the **Microsoft Fabric Analytics Engineer Associate certification**.

I passed with a score of 880!! Here are my [Microsoft Fabric Analytics Engineer Associate certification credentials](https://learn.microsoft.com/en-us/users/dazhonhunt-8403/credentials/certification/fabric-analytics-engineer-associate?tab=credentials-tab).

# Data Flow

1. **Raw Data**: The raw news data is retrieved in **JSON format** from the **Bing News Search API** through **Microsoft Fabric** which is sourced from the **Bing News API** in [Azure](https://azure.microsoft.com/en-us/products/app-service/api/?ef_id=_k_CjwKCAiA3Na5BhAZEiwAzrfagHOq3ur7AgTzW9WBvCJLv7gvlZ40sDvre49F4NtOaRsNCiEESxuBnBoCF_YQAvD_BwE_k_&OCID=AIDcmm5edswduu_SEM__k_CjwKCAiA3Na5BhAZEiwAzrfagHOq3ur7AgTzW9WBvCJLv7gvlZ40sDvre49F4NtOaRsNCiEESxuBnBoCF_YQAvD_BwE_k_&gad_source=1&gclid=CjwKCAiA3Na5BhAZEiwAzrfagHOq3ur7AgTzW9WBvCJLv7gvlZ40sDvre49F4NtOaRsNCiEESxuBnBoCF_YQAvD_BwE)

2. **Data Transformation**
      
  - **Process**:
     - Read and processed multi-line JSON data containing nested structures.
     - Extracted key fields like titles, descriptions, categories, and image URLs.
     - Converted the processed data into a Delta table for incremental data updates

[Transformation Notebook](<https://github.com/DazhonH/Microsoft-Fabric-Data-Engineering-Project/blob/main/Bing-News-Transformation.ipynb>)

3. **Sentiment Analysis**:

- **Process**:
    - Queried the cleaned data from the Delta table.
    - Applied SynapseML's AnalyzeText for sentiment analysis on news descriptions.
    - Analyzed the sentiment of news descriptions to categorize them as positive, neutral, or negative.
    - Stored the resulting sentiment data in a new Delta table for further use.

[Sentiment Analyis Notebook](<https://github.com/DazhonH/Microsoft-Fabric-Data-Engineering-Project/blob/main/Bing%20-%20Sentiment%20-%20Analysis.ipynb>)

 
4. **Visualization**: The transformed data is used to generate interactive visualizations in Power BI. These visualizations allow users to analyze trends in the news data, including article distribution by category, frequency of topics, and more.

## Technologies Used

- **Azure Portal**
- **Apache Spark**
- **Python**
- **Jupyter Notebooks**
- **Power BI**

## End Result
### Page 1 - Summary

![Bing News Dashboard - Summary Page](https://github.com/user-attachments/assets/3a0c05b1-6e9c-43e7-9164-fbb7298d48f9)

### Page 2 - News Articles

![Bing News Dashboard - News Page](https://github.com/user-attachments/assets/db9c38bf-ef29-4f3c-9db8-74f32483d4e1)

## Conclusion
This project provided valuable experience in building end-to-end data pipelines using Microsoft Fabric, Apache Spark, and Power BI. Future improvements could include adding real-time data processing, incorporating machine learning for more advanced predictions, and enhancing the visualizations for deeper insights.



