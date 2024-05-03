import PyPDF2 as pdf
import os

import PyPDF2
import pandas as pd
import re
import glob

def extract_titles_from_text(text):
    pattern = re.compile(r'(?P<title>[A-Z][\w\s]+)\nLEARNING OUTCOMES', re.MULTILINE)
    titles = []
    for match in pattern.finditer(text):
        title = match.group("title").strip()
        lines = title.split("\n")
        last_line_words = lines[-1].split()
        if len(last_line_words) == 1 and len(lines) > 1:
            pre_last_line_words = lines[-2].split()
            if len(pre_last_line_words) >= 2 and all(word[0].isupper() for word in pre_last_line_words):
                if pre_last_line_words[0] != "Level":
                    title = f"{lines[-2]} {last_line_words[0]}"
        titles.append(title.replace("Level III Topic Outlines", ""))
    return titles

def clean_learning_outcome(val):
    val = val.replace('\t', ' ')
    cleaned_val = re.sub(r'[^\w\s.-]', '', val)
    if not cleaned_val.endswith('.'):
        cleaned_val += '.'
    cleaned_val = cleaned_val.capitalize()
    return cleaned_val

def clean_topics(val):
    cleaned_val = re.sub(r'\d+', '', val)
    return cleaned_val

def process_dataframe(df):
    df['Learning Outcomes'] = df['Learning Outcomes'].apply(clean_learning_outcome)
    df['Topic'] = df['Topic'].apply(clean_topics)
    return df

def read_pdf_text(pdf_path):
    text = ""
    with open(pdf_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        for page in reader.pages:
            text += page.extract_text()
    return text

def get_data_from_pdf(pdf_path):
    text = read_pdf_text(pdf_path)
    titles_before_outcomes = extract_titles_from_text(text)
    lines = text.split("\n")
    data = []
    current_topic = ""
    current_heading = ""
    outcome = ""
    for i, line in enumerate(lines):
        line = line.strip()
        if not line or line == "LEARNING OUTCOMES":
            continue
        if line[0].isupper() and not line.startswith("□") and not "The candidate should be able to" in line:
            if current_topic != '' and current_heading != '' and outcome != '':
                data.append([pdf_path.split('/')[-1], current_topic, current_heading, outcome.strip("□ ").replace("\n", " ")])
                outcome = ""
            for val in titles_before_outcomes:
                if val.strip() in line:
                    current_topic = line
                    current_heading = ""
                else:
                    current_heading = line
        elif line.startswith("□") or "The candidate should be able to" in line:
            if outcome:
                data.append([pdf_path.split('/')[-1], current_topic, current_heading, outcome.strip("□").replace("\n", "")])
                outcome = ""
            outcome = line
        if i == len(lines) - 1 and outcome:
            data.append([pdf_path.split('/')[-1], current_topic, current_heading, outcome])
    return data

def process_all_pdfs(pdf_path):
    pdf_files = glob.glob(pdf_path)
    data_corrected = []
    for pdf in pdf_files:
        data = get_data_from_pdf(pdf)
        data_corrected.extend(data)
    df = pd.DataFrame(data_corrected, columns=["File Name", "Topic", "Heading", "Learning Outcomes"])
    processed_df = process_dataframe(df)
    processed_df.to_csv('/opt/airflow/src/dataset/final_output.csv', index=False)
    return processed_df

# Now, to execute everything, simply call:

if __name__ == "__main__":
    process_all_pdfs("../dataset/*.pdf")

#process_all_pdfs("/Users/riyasingh/Downloads/Datasets/Sample_PDFs/*.pdf")
