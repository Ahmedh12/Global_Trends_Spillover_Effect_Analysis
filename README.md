# Project Title: Influence of Global Index Stock Prices on Egyptian Index Stocks

## Overview
This project aims to analyze the influence of global stock market indices on the Egyptian EGX 30 index using PySpark for data processing and advanced econometric models such as GARCH and Granger causality tests for analysis.

## Project Workflow

### 1. Data Collection
- Historical data for global indices (e.g., S&P 500, FTSE 100) and EGX 30 was sourced from platforms such as Yahoo Finance and Investing.com.
- Exchange rates were integrated to convert EGX 30 data (EGP) into USD for consistency.

### 2. Data Preprocessing
- Merging and aligning data by date across all indices.
- Handling missing values using forward-fill or dropping missing rows.
- Calculating daily returns for each index using the formula:

  ![Daily Return](https://latex.codecogs.com/svg.latex?\text{Daily%20Return}=\frac{\text{Current%20Close}-\text{Previous%20Close}}{\text{Previous%20Close}})


### 3. Descriptive Analytics
- Summary statistics were computed for daily returns to provide insights into the distribution and variability of each index.
- Visualization tools such as Matplotlib were used to plot time series and return distributions.

### 4. Correlation Analysis
- A correlation matrix was computed to measure the linear relationship between the indices.
- Heatmaps were used for visualizing correlations.

### 5. Granger Causality Analysis
- Granger causality tests were conducted to determine whether past values of one index can predict another index.
- P-values for different lags were plotted to interpret significant causal relationships.

### 6. Predictive Analytics
- We trained a Gradient Boosting Regressor for each stock index using its respective data and evaluated its performance on the test split of the same dataset.  
- Each trained model was cross-validated against EGX_30 data to compare its performance with the results obtained on its own test dataset.

## Prerequisites
- Python 3.9+
- Apache Spark with PySpark
- JDK 8 or later (for Spark compatibility)
- Required Python libraries:
  - `pandas`
  - `numpy`
  - `matplotlib`
  - `statsmodels`
  - `arch`

## Installation
1. Clone this repository.
   ```bash
   git clone https://github.com/Ahmedh12/Global_Trends_Spillover_Effect_Analysis.git
   ```
2. Set up a Conda environment.
   ```bash
   conda create -n my_pyspark_env python=3.9
   conda activate my_pyspark_env
   ```
3. Install required libraries.
   ```bash
   pip install pandas numpy matplotlib statsmodels arch pyspark
   ```
4. Ensure Spark is configured correctly with Hadoop and JDK.

## Usage
1. Run `download_data.py` to download the indices data in `data/raw` directory
2. Run the PySpark script to preprocess data and calculate daily returns.
3. Use Jupyter Notebook or the provided scripts to conduct analyses (correlation, Granger causality).
4. Visualize and interpret the results.

## Results
- **Descriptive Statistics**: Summary tables and histograms for returns.
- **Correlation Analysis**: Heatmaps showing relationships between indices.
- **Granger Causality**: Lag-specific causal relationships.
- **GARCH Model**: Insights into volatility dynamics.

## Limitations
- Assumes linear relationships in correlation and Granger causality.
- GARCH modeling is limited to univariate analysis in this project.

## References
1. Investing.com. (n.d.). Historical data of global indices.
2. Yahoo Finance. (n.d.). Historical data for S&P 500. Retrieved from https://finance.yahoo.com/quote/%5EGSPC/history/
3. PySpark documentation. Apache Spark: Unified Analytics Engine for Big Data. Retrieved from https://spark.apache.org/docs/latest/api/python/

## License
This project is licensed under the MIT License.

