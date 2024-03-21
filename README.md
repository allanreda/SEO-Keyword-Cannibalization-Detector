# SEO-Keyword-Cannibalization-Detector

## Overview  
This script is built to solve the SEO challenge of keyword cannibalization by analyzing search performance data from Google Search Console. It evaluates keyword overlaps across different URLs, identifying instances where multiple URLs might be competing for the same keywords. The script facilitates the identification of cannibalization issues, enabling SEO specialists to optimize their website's content strategy. It fetches URLs, extracts and processes HTML content, analyzes keyword performance data, and finally provides an overview of possible cannibalized keywords.

## Functionality  
Initially, the script authenticates with Google's Search Console API to access search analytics data. It then processes a list of predefined URLs from a local Excel file, removing irrelevant parameters and filtering out non-essential HTML elements to focus on the content meaningful for SEO. The script fetches keyword data for each URL, identifying which keywords have multiple ranking URLs, and therefore are prone to cannibalization. This analysis includes aggregating data on clicks, impressions, CTR, and average positions to understand the impact of keyword overlaps. The final output is a DataFrame that details the cannibalized keywords, their corresponding URLs, and associated search performance metrics. This DataFrame is exported to an Excel file at the end. The final DataFrame looks like this: 
![image](https://github.com/allanreda/SEO-Keyword-Cannibalization-Detector/assets/89948110/c96f781d-2c11-4c72-8d51-232f26f961f3)

## Technologies  
The script is built using:  
-Google API Client Library for Python to import data from Google Search Console API.  
-Pandas for data manipulation and analysis.  
-Requests and Beautiful Soup for scraping and processing HTML content from web pages.  
-Concurrent.futures and multiprocessing for parallel processing, enhancing efficiency in data fetching  

## Goal  
The primary objective of this script is to enhance a website's SEO efficiency by resolving keyword cannibalization issues. By automating the detection and analysis of keyword overlaps, the script aids SEO professionals in making informed decisions on content optimization and site architecture adjustments. This proactive approach to SEO management supports improved page rankings, better user experience, and ultimately, higher website traffic.
