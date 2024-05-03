#!/usr/bin/env python
# coding: utf-8


#pydantic model

import pandas as pd
from pydantic import BaseModel, Field, ValidationError, validator
from typing import List, Optional
import re

# Define your Pydantic models with validation
class LearningOutcome(BaseModel):
    file_name: str = Field(..., description="File Name")
    topic: str = Field(..., description="The main topic from the PDF")
    heading: Optional[str] = Field(None, description="The subheading related to the learning outcome")
    outcome: str = Field(..., description="The detailed learning outcome")


    @validator('topic', pre=True)
    def validate_topic(cls, value):
        if not isinstance(value, str) or not value.strip():
            raise ValueError('The "Topic" field must not be empty and should be a string.')
        return value.strip()
    

    @validator('heading', pre=True, always=True)
    def validate_heading(cls, value):
        if value is not None:
            return value.strip()
        return value

    @validator('outcome', pre=True)
    def validate_outcome(cls, value):
        if not value:
            raise ValueError('The "Learning Outcomes" field must not be empty.')
        # Add more complex validation as necessary
        return value.strip()

class ProcessedTextData(BaseModel):
    outcomes: List[LearningOutcome] = Field(..., description="List of learning outcomes from the CSV file")

# Function to read the CSV, validate, clean, and save the clean data
def validate_and_save_csv(input_csv_path: str, output_csv_path: str):
    df = pd.read_csv(input_csv_path)
    cleaned_data = []

    for index, row in df.iterrows():
        try:
            outcome = LearningOutcome(
                file_name = row['File Name'],
                topic=row['Topic'],
                heading=row['Heading'] if 'Heading' in row and pd.notnull(row['Heading']) else None,
                outcome=row['Learning Outcomes']
            )
            cleaned_data.append(outcome.dict())
        except ValidationError as e:
            print(f"Row {index} validation error: {e}")

    # Convert cleaned data to DataFrame
    clean_df = pd.DataFrame(cleaned_data)
    
    # Save the cleaned DataFrame to a new CSV file
    clean_df.to_csv(output_csv_path, index=False)
    print(f"Cleaned data saved to {output_csv_path}")

# Specify the path to your input CSV file and the path where the cleaned CSV file will be saved
input_csv_path = '../dataset/final_output.csv'  # Replace with the path to your input CSV
output_csv_path = '../dataset/Cleanedfinal_output.csv'  # Replace with the path for the cleaned output CSV

# Call the function to validate and save the cleaned data to CSV

if __name__ == "__main__":
    validate_and_save_csv(input_csv_path, output_csv_path)





