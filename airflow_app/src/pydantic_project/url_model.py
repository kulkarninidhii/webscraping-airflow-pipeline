from pydantic import BaseModel, HttpUrl, ValidationError, Field, field_validator
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse
import re
import json
import csv

#Pydantic Class for URLModel with the schema provided for extraction
class URLModel(BaseModel):
    Name_of_the_topic: str = Field(alias="Name of the topic")
    Year: Optional[int] = Field(alias="Year")
    Level: int = Field(alias="Level")
    Introduction_Summary: Optional[str] = Field(alias="Introduction Summary")
    Learning_Outcomes: Optional[str] = Field(alias="Learning Outcomes")
    Link_to_the_Summary_Page: HttpUrl = Field(alias="Link to the Summary Page")
    Link_to_the_PDF_File: Optional[HttpUrl] = Field(alias="Link to the PDF File")
    
    
    #field validation for Year to check int/none input and check the year is appropriate
    @field_validator('Year', mode='before')
    def validate_year(cls, value):
        if value is None:
            return value
        
        #extract current year for comparison 
        current_year = datetime.now().year
        if isinstance(value, str):
            cleaned_year = ''.join(filter(str.isdigit, value))
            if cleaned_year:
                value = int(cleaned_year)
            else:    
                raise ValueError("Year must contain digits")
        
        if not isinstance(value, int):
            raise TypeError("Year must be provided as an integer or string containing digits.")
        
        if not (2010<= value <=current_year):
            raise ValueError(f"Invalid Year {value}.")
    
        return value    

    #field validation for Level ; map the roman numerals to integers and pass through the int value
    @field_validator('Level', mode='before')
    def validate_level(cls, v):
        roman_to_int = {'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'V': 5}
        roman_numeral = v.replace("Level ", "").strip()
        
        if roman_numeral in roman_to_int:
            return roman_to_int[roman_numeral]
        else:
            raise ValueError("Invalid Roman numeral")
    
        
    #field validation for summary page link to check if the link is a valid link
    @field_validator('Link_to_the_Summary_Page', mode='before')
    def validate_url_domain(cls, v):
        parsed_url = urlparse(v)
        expected_domain = "www.cfainstitute.org"
        if parsed_url.netloc.lower() != expected_domain:
            raise ValueError(f"URL must be from {expected_domain}")
        return v
    
    #field validation for pdf link to check if the link is a valid link and has valid extension
    @field_validator('Link_to_the_PDF_File', mode='before')
    def validate_url_domainpdf(cls, v):
        if v is None:
            return v
        
        parsed_url = urlparse(v)
        expected_domain = "www.cfainstitute.org"
        if parsed_url.netloc.lower() != expected_domain:
            raise ValueError(f"URL must be from {expected_domain}")
        if not parsed_url.path.lower().endswith('.pdf'):
            raise ValueError("URL must end with .pdf")
        return v

    #clean the learning outcomes
    @field_validator('Learning_Outcomes', mode='before')
    def clean_learning_outcomes(cls, v):
        v = v.strip()
        v = re.sub(r'\s+', ' ', v)
        return v
    
    #clean the introduction summary
    @field_validator('Introduction_Summary', mode='before')
    def clean_intro_summary(cls, v):
        v = v.strip()
        v = re.sub(r'\s+', ' ', v)
        return v



def validate_and_store(json_file_path: str, csv_file_path: str):
#Use the data in JSON for validation
    with open(json_file_path, 'r') as file:
        json_data = json.load(file)

    #Validate each item in the dataset; if cleared save it to csv, if not then omit 
    validated_data = []
    for item in json_data:
        try:
            validated_item = URLModel.model_validate(item)
            validated_data.append(validated_item.model_dump())
        except (ValidationError, TypeError) as e:
            print(f"Error for item {item}: {e}")
            continue

    with open(csv_file_path, 'w', newline='') as file:
        if validated_data:
            fieldnames = validated_data[0].keys()
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for item in validated_data:
                writer.writerow(item)
        else:
            print("No validated data to save.")

if __name__ == "__main__":
    json_file_path = '/opt/airflow/dataset/CFA.json'
    csv_file_path = '/opt/airflow/dataset/validated_CFA.csv'
    validate_and_store(json_file_path, csv_file_path)
