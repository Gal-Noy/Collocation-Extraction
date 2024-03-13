# Collocation Extraction

## Overview
This project implements an automated collocation extraction system using **Amazon Elastic Map Reduce** and **Apache Hadoop** on the Google 2-grams dataset. Collocations are identified based on the Normalized Pointwise Mutual Information (NPMI) value calculated for pairs of ordered words.

## System Architecture
The system is composed of three main components:
1. **Local Application**: Used to initiate the map-reduce job on Amazon EMR and manage interactions with the AWS services.
2. **Amazon Elastic Map Reduce (EMR)**: Utilized to distribute the collocation extraction task across multiple nodes and process the Google 2-grams dataset.
3. **Amazon S3**: Used for storing input datasets, intermediate data, and output results.

## Application Flow
The system comprises three main steps:
1. Step One - **Preprocessing**: Tokenizes input text, filters out stop words, and generates word pairs with their co-occurrence counts.
2. Step Two - **Calculation**: Calculates the significance of word pairs using normalized pointwise mutual information (NPMI).
3. Step Three - **Analysis**: Filters significant collocations based on predefined thresholds (minPmi, relMinPmi) and outputs the results.

## Running the Application:
To run the application, follow these steps:
1. **Compile the Code**: Compile the Java code into a JAR file using Maven.
2. **Run the Local Application**: Execute the following command on your local machine:
   
   ```
   java -jar CollationExtraction.jar <minPmi> <relMinPmi>
   ```
- `minPmi`: Minimal PMI value for collocation determination.
- `outputFileNameI`: Relative minimal PMI value for collocation determination.

## Details
### Instance Details
- **Instance type**: M4XLarge

### Time Taken (including manager and workers initiation)
- Full Hebrew corpus: `minPmi = 0.5, relMinPmi = 0.2` => 65 mappers - **2 hours, 1 minute**
- Half Hebrew corpus: `minPmi = 0.5, relMinPmi = 0.2` => 39 mappers - **1 hour, 20 minutes**

### Other Details:
- The system utilizes Amazon EMR for scalable distributed processing.
- Input datasets are stored in Amazon S3 and accessed directly by EMR nodes.
- Stop words are filtered out during collocation extraction.
- Results are stored in Amazon S3 and can be accessed for further analysis.

## References:
- AWS documentation for AWS SDK and services.
- Google 2-grams dataset.
