## Vacation home business Data Platform: Case Study 
Data Platform design and implantation for vacation home data analysis

### Business Wish
To identify the market trends from the AirBnb data, to optimize their processes  and highlight the pain points.

### Data Platform

Microsoft Azure is the preferred cloud provider as it provides most of the data analytics and transformation services natively. moreover scalability and high availability options are there.
The diagram below shows the basic architecture of the data platform.
![Screenshot](dataPlatformDesign.jpg)
#### Production Approach
* In a production environment where source files can be onboarded with different frequencies and from different sources, DataFactory can onboard and integrate different data sources. 
* Depending on whether it's a pull or a push mechanism in Data lake container, different strategies can be optimized.
![Screenshot](DL.png)
* Once the data is inside Azure premises, local services can use it without additional approvals. DataBricks can be chosen as a transformation engine as it has enterprise scale compute power along with Git and notebook support, where **jobs can be scheduled** and insights can be generated. 

* Since the intermediate and final dataset is going to be on Azure it can easily be utilized for different usecases such as machine learning and power bi for dashboard and reporting.

* Orchestrator notebook can be run on daily basis along with the DataFactory pipeline for data onboarding.<br>
  *Ingestion Pipeline on DataFactory (onboards new data)*
==>> *Orchestration notebook on DataBricks to update the data in the tables*<br>


### Additional Actions 
* Additional Data Quality checks can be implemented at Silver layer tables, which will make sure business can be aware of data quality rules and can be assured of the correct data getting to the gold layer.
* Depending on use case more gold layer datasets can be introduced.
* References can found out at the end of the presentation.

======================================================
