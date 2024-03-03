# Automating synthetic data creation & reporting using Microsoft Fabric 
![](https://img.shields.io/badge/Hack%20Together-The%20Microsoft%20Fabric%20Global%20AI%20Hack-informational?style=flat&logo=<LOGO_NAME>&logoColor=white&color=2bbc8a)



## Table of Contents
* [General Info](#general-information)
* [Technologies Used](#technologies-used)
* [Demo Instruction](#Demo-instructions)
* [Setup](#setup)
* [Usage](#usage)
* [Acknowledgements](#acknowledgements)


## General Information 
When considering privacy protection in the context of an outsourcing company, synthetic data generation becomes particularly relevant due to the sensitive nature of the data involved. Outsourcing companies often handle data from clients that may contain personally identifiable information (PII), financial records, proprietary business information, and other sensitive data.

Here's how synthetic data generation can enhance privacy protection for outsourcing companies:

1. **Compliance with Regulations**: Outsourcing companies are often subject to strict data privacy regulations such as GDPR (in the European Union), HIPAA (in healthcare), or CCPA (in California). These regulations impose stringent requirements on how personal data should be handled, processed, and stored. By using synthetic data, outsourcing companies can reduce the need to work directly with real, sensitive data while still complying with regulatory requirements.

2. **Minimization of Data Exposure**: Handling real data increases the risk of data breaches and unauthorized access. Even with robust security measures in place, the potential for data leaks remains a concern. Synthetic data generation allows outsourcing companies to minimize the exposure of actual sensitive data by creating realistic yet entirely synthetic datasets for tasks like software development, testing, and analytics.

3. **Secure Collaboration**: Outsourcing often involves collaboration with external partners, vendors, or remote teams. Sharing sensitive data with these parties increases the risk of data misuse or breaches. Synthetic data provides a secure alternative for collaboration, as it can be freely shared with external stakeholders without compromising the confidentiality of the original data.

4. **Data Masking and Anonymization**: Even within the outsourcing company itself, access to sensitive data may need to be restricted to specific roles or individuals. Synthetic data can be used to mask or anonymize real data, allowing employees who do not require access to sensitive information to work with realistic but non-sensitive datasets. This reduces the risk of internal data breaches and unauthorized access.

5. **Ethical Considerations**: In addition to legal compliance, outsourcing companies often have ethical responsibilities to protect the privacy and confidentiality of their clients' data. Synthetic data generation aligns with these ethical considerations by providing a way to fulfill business objectives without compromising individual privacy rights.

By leveraging synthetic data generation techniques, outsourcing companies can effectively manage and mitigate the privacy risks associated with handling sensitive data, thereby building trust with clients, enhancing data security, and ensuring regulatory compliance.

## Technologies Used
- Microsoft Fabric
- Synapse Notebook
- OpenAI
- Data Pipeline
- Lakehouse

## Demo Instructions

1. **Original Data**: First step is to add tables to the Lakehouse. Analyze the tables taking into account the type of the data for each column and save the resulted statistics into a csv.

2. **Synthetic Data**: Based on the real dataset, we aim to create a synthetic one, which preserves its underlying structure and  reproduces its statistical properties. With this scope in mind, we proposed a solution that, given a lakehouse with multiple tabular datasets, trains a custom Conditional Tabular Generative Adversarial Network (CTGAN) on each of the respective real data and then uses it to sample a fake dataset. The hyperparameters configuration used in the training process is provided by OpenAI, based on the size of the target table. The resulting synthetic data is further compared with the real one, and it can be visualized in the reporting folder for each specific table. 

3. **Summary Synthetic Data**: After the synthetic data is created we analyze again the data and save the resulted statistics of the synthetic data into a csv.

4. **Decision Tree Classifier & Regressor**: As a minimum viable product, we have implemented a Decision Tree Classifier and Regressor to predict the target column of the synthetic data. The model is trained and then used to predict the target column of the synthetic data. In the same time the variable importance is calculated and alltogether with the model metrics are passed to the report. 

5. **Report**: Then we create a report using the synthetic data and the real data. The report is created in the following steps:
    - *Data Distribution*: We compare the data distribution of the synthetic data and the real data. We compare the mean, the standard deviation, the minimum, the maximum, and the quantiles of the synthetic data and the real data.
    - *Data Correlation*: We compare the data correlation of the synthetic data and the real data. We compare the correlation matrix of the synthetic data and the real data.
    - *Data Visualization*: We visualize the synthetic data and the real data using histograms, scatter plots, and box plots.

All the steps (including parameters setting) are implemented in the mainflow.

![pipeline](image-15.png)


## Setup
- To run this project, you need to have access to Microsoft Fabric.
- In order to use all the notebooks in your workspace you need to create an enviroment and to install the following python libraries:
    - *openai 1.12.0*
    - *pdf2image* 1.17.0
    - *reportlab* 4.1.0
- Next we need to create a pipeline and to add the following variables as in the image:

![variables](image-10.png)

- Create the activies as in the image above and connect them. A special case if the foreach loop for prediction that contains an if function (detailed in the image below).

![foreachloop](image-11.png)

Now we take a closer look to each actitivity and to it's settings:  
-   *import_data_to_lakehouse*: This activity is used to import the data from the source to the lakehouse. The source and the destination are set as parameters (more details in the Usage part)  
-  *summary_raw_data*: This activity is used to create the summary of the original data. For this activity in the settings point it to the notebook `summary_raw_data`.

![Alt text](image-29.png)

- *Gpt_prompting_configuration*: This activity use dataset no of rows and columns and asks Chatgpt using OpenAI API for the batch size and the no of epochs for each dataset. For this activity we have the following settings:

![Alt text](image-13.png) 


With the value dict_configurations being:

![Alt text](image-14.png)

 *Synthehtic_data_generation* : This activity is used to generate the synthetic data. For this activity in the settings point it to the notebook ` Synthetic_data_generation`.

![Alt text](image-16.png)

With the value dict_configurations being:


![Alt text](image-20.png)
![Alt text](image-21.png)


*Summary_synthetic_data* : After the synthetic data is created we analyze again the data and save the resulted statistics of the synthetic data into a csv.

![Alt text](image-22.png)

- *Report_prompting*: Based on the sumary files, this activity is used to  generate prompts for Chatgpt using OpenAI API. Further they will be used to generate the final report.

- *Gpt_prompting*: This activity is used to generate the report based on the prompt composed in the previous step (notebook *Report_prompting*). For this activity we have the following settings:

![Alt text](image-23.png)
![Alt text](image-24.png)

- *ForEach*: Based on the number of the variables number, this activity will perform either Regression or Classification. 

![Alt text](image-25.png)
![Alt text](image-26.png)

- *Generate_reports*: This activity is used to generate the reports for each dataset. For this activity we have the following settings:

![Alt text](image-27.png)
![Alt text](image-28.png)

## Usage
- In the mainflow, the first parameters we set are for `import_data_to_lakehouse`. These parameters as the names suggests define the source and the destinations of the data to be processed.

![Alt text](image-6.png)
![Alt text](image-5.png)

- The second parameter that we set is `synthetic_dataset_len`. This parameter will define the number of the rows in the generated synthetic dataset.

![Alt text](image-7.png)

The last parameter we set is `label_variable`. This parameter will define the target column of the synthetic dataset, for each dataset that we want to pass through to the decision tree model.

![Alt text](image-8.png)


## Room for Improvement

- In terms of original data processing, the current implementation is limited to tabular datasets. A potential improvement would be to extend the solution to handle other types of data, such as images, audio, or text. This could involve integrating additional data processing and feature extraction techniques tailored to these data types.

- In terms of synthetic data generation, there remains potential for enhancement: 
Presently, our trained CTGAN falls short in replicating time series datasets. Thus, a prospective improvement would entail integrating a TimeGAN (Time-series Generative Adversarial Network) to effectively manage such inputs. The GAN model that was used (CTGAN) is more suitable for numerical data. It can be used for string data but it will increase the computational time.

## Acknowledgements
- This project was based on [live demos from Microsoft Fabric](https://blog.fabric.microsoft.com/en-us/blog/hack-together-the-microsoft-fabric-global-ai-hack/).




