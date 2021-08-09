# Spark-Example

This spark example code is used to calculate the number of people per each Olympic medal in 2021 by country. To this end, two dataset are utilized to get information about the total number of medals in recent Olympic Games as well as the current population by country.
To join these table, both dataset contain ISO codename for countries.

## Datasets

### Olympic medal table 2021 dataset
This dataset shows the number of medals in Olympic Tokyo 2021 by country. More than 11,000 athletes from 200 countries took part in these Olympic Games. Its schema has 8 columns and be structured as follows:

| Rank | Team/NOC                   | Gold Medal | Silver Medal | Bronze Medal | Total | Rank by Total | NOCCode |
|------|----------------------------|------------|--------------|--------------|-------|---------------|---------|
| 1    | United States of America   | 39         | 41           | 33           | 113   | 1             | USA     |
| 2    | People's Republic of China | 38         | 32           | 18           | 88    | 2             | CHN     |
| 3    | Japan                      | 27         | 14           | 17           | 58    | 5             | JPN     |
| 4    | Great Britain              | 22         | 21           | 22           | 65    | 4             | GBR     |
| 5    | ROC                        | 20         | 28           | 23           | 71    | 3             | ROC     |

### World population 2021 dataset
The world population 2021 dataset contains data population, growth rate, desity,... by country in 2021. Below is the schema of this dataset with 9 columns.

| iso_code | country       | 2021_last_updated | 2020_population | area            | density_sq_km | growth_rate | world_% | rank |
|----------|---------------|-------------------|-----------------|-----------------|---------------|-------------|---------|------|
| CHN      | China         | 1,444,725,231     | 1,439,323,776   | 9,706,961 sq_km | 149/sq_km     | 0.34%       | 18.34%  | 1    |
| IND      | India         | 1,394,820,952     | 1,380,004,385   | 3,287,590 sq_km | 424/sq_km     | 0.97%       | 17.69%  | 2    |
| USA      | United States | 333,119,377       | 331,002,651     | 9,372,610 sq_km | 36/sq_km      | 0.58%       | 4.23%   | 3    |
| IDN      | Indonesia     | 276,661,173       | 273,523,615     | 1,904,569 sq_km | 145/sq_km     | 1.04%       | 3.51%   | 4    |
| PAK      | Pakistan      | 225,651,101       | 220,892,340     | 881,912 sq_km   | 255/sq_km     | 1.95%       | 2.86%   | 5    |

## Results

This code is run in the Spark standalone mode. The version of Scala is 2.12.12 and Spark is 3.0.0
After running the code, all result is stored in **data/tatistic_olympic_2021.csv**. 
Surprisingly, the country that has the best number of medals is San Mario just with a small population. It turns out that countries such as The US, China, or British did not get many medals with their big population.

| country_name | number_of_people_per_medal |
|--------------|----------------------------|
| San Marino   | 11310                      |
| New Zealand  | 241112                     |
| Jamaica      | 329019                     |
| Bahrain      | 437479                     |
| Hungary      | 483018                     |
| Georgia      | 498646                     |
| Australia    | 554345                     |
| Estonia      | 663268                     |
| Norway       | 677655                     |
| Armenia      | 740811                     |
| Cuba         | 755108                     |



