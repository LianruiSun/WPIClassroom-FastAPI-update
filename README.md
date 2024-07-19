# WPI IQP Healthy Classroom FastAPI Fetch

## Overview
This Python-based system fetches air quality data from various devices using the Awair API, processes the data, and stores it in AWS DynamoDB. It's designed to help environmental analysts and enthusiasts monitor air quality in real-time across multiple locations.

## Features
- **Data Retrieval**: Gathers data from configured devices via the Awair API.
- **Data Processing**: Structures and formats the data for analytical use.
- **DynamoDB Storage**: Stores data in AWS DynamoDB with dynamic table creation based on device ID and date.
- **Lambda Deployment**: Deploys data in AWS Lambda with trigger event collecting data continually.
