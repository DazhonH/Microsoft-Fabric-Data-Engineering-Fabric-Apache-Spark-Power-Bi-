# End-to-End Data Pipeline with Microsoft Fabric, Apache Spark, and Power BI

![Microsoft Fabric Data Pipeline](https://github.com/user-attachments/assets/403f9e09-0dfc-42e4-bf3b-d0b69a4f93ec)


## Project Overview
This project demonstrates a pipeline where data from the Bing News Search API is processed using Apache Spark and visualized using Power BI. The pipeline cleans, aggregates, and prepares the data for data visualization. The main purpose of this project was to gain hands on experience using Microsoft Fabric in order to pass the DP-600 which is the **Microsoft Fabric Analytics Engineer Associate certification**.

I successfully passed the certification.  Here are my [Microsoft Fabric Analytics Engineer Associate certification credentials](<https://learn.microsoft.com/en-us/users/dazhonhunt-8403/credentials/certification/fabric-analytics-engineer-associate?tab=credentials-tab&source=docs>)
## Data Flow

1. **Data Ingestion**:

   - Created a resource group in Azure called **bing-fabric-data-analytics**. Within this resource group, I created a Bing search resource that you can find via marketplace.
     <br><br>
   
    <img src="https://github.com/user-attachments/assets/9fa00d19-6a45-4f05-81c0-b747a94d39fe" width="450"/>
    <br><br>

   - For the storage destination I created a lakehouse in Fabric called **bing_lake_db**.
   - To configure the data source I connected to the Bing Search API via Rest connection in Fabric.
    <br><br>
   
   <img src="https://github.com/user-attachments/assets/486b9ce9-92ee-4612-a772-ce65fdff8d6d" width="400"/>
   <br><br>

  **Paramaters for a successfull connection to the API**
  - The [Endpoints](<https://learn.microsoft.com/en-us/bing/search-apis/bing-news-search/reference/endpoints>) for the base URL.
    (https://api.bing.microsoft.com/v7.0/news/search)
  
  - [Query paramaters](<https://learn.microsoft.com/en-us/bing/search-apis/bing-news-search/reference/query-parameters>) to define the search.
    (?q=latest+news&count=100&freshness=Day&mkt=en-US)

  - The [headers](<https://learn.microsoft.com/en-us/bing/search-apis/bing-news-search/reference/headers>) ( Ocp-Apim-Subscription-Key).
    This is the subscription key that you receive when you sign up in Azure Portal while creating a [Bing resource](<https://learn.microsoft.com/en-us/bing/search-apis/bing-web-search/create-bing-search-service-resource#create-your-bing-resource>).

  - I set the destination up to the Fabric lakehouse **bing_lake_db** that I created earlier, the file path to **bing-latest-news.json**, and the file format to JSON.

     <img src="https://github.com/user-attachments/assets/a73120c7-2fd8-467f-8a9c-c3e557cc8d46" width="550"/>
     <br><br>
     
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

## Lakehouse Snapshot
![Screenshot 2024-12-21 152426](https://github.com/user-attachments/assets/7892b31e-5463-4427-b12e-ba5d4892ee2a)

 


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

## About This Project
This project was inspired by the udemy course [Build Bing News Data Analytics platform using different Data Engineering components of Microsoft Fabric [DP-600][DP-203]](<https://www.udemy.com/course/microsoft-fabric-end-to-end-data-engineering-project/learn/lecture/43202384#overview>). I followed the tutorial as part of my preperation for the Fabric Analytics Engineer Associate certification that I successfully passed. While the base structure follows the tutorial, I added customizations to explore additional features and solidify my understsnding.


