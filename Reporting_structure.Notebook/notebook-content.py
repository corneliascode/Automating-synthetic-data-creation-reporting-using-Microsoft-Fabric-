# Synapse Analytics notebook source

# METADATA ********************

# META {
# META   "synapse": {
# META     "lakehouse": {
# META       "default_lakehouse": "e0ad8bbb-7fef-4aea-8974-29afb80ca048",
# META       "default_lakehouse_name": "lakehouse1",
# META       "default_lakehouse_workspace_id": "2ee099f6-da9f-4900-94cf-ffa73f8cbaa1"
# META     },
# META     "environment": {
# META       "environmentId": "3555fd2b-975d-4c87-bb07-a6fa3c8ac2eb",
# META       "workspaceId": "2ee099f6-da9f-4900-94cf-ffa73f8cbaa1"
# META     }
# META   }
# META }

# CELL ********************

from reportlab.platypus import SimpleDocTemplate, Paragraph, PageBreak
from reportlab.lib.styles import getSampleStyleSheet
import reportlab.platypus as rpl
import os
import datetime
from pdf2image import convert_from_path
import ast
import matplotlib.image as mimg

# PARAMETERS CELL ********************

gpt_response = ''

# CELL ********************

#gpt_response = "[{'dataset_name': 'diabetes', 'prompt': '--- diabetes_summary.csv ---\\nMean:\\n- BMI: 26.37579185520364\\n- BP: 94.64701357466062\\n- S2: 115.43914027149316\\n- S3: 49.78846153846154\\n- S4: 4.07024886877828\\n- S5: 4.641410859728507\\n\\nStandard Deviation:\\n- BMI: 4.4156190843115635\\n- BP: 13.823449217274112\\n- S2: 30.395854640525904\\n- S3: 12.926876069792483\\n- S4: 1.2897189705249357\\n- S5: 0.5220946728774889\\n\\nMinimum Value:\\n- BMI: 18.0\\n- BP: 62.0\\n- S2: 41.6\\n- S3: 22.0\\n- S4: 2.0\\n- S5: 3.2581\\n\\nMaximum Value:\\n- BMI: 42.2\\n- BP: 133.0\\n- S2: 242.4\\n- S3: 99.0\\n- S4: 9.09\\n- S5: 6.107\\n\\nSkewness:\\n- BMI: 0.5961166556214369\\n- BP: 0.2896710382759045\\n- S2: 0.43510875835701934\\n- S3: 0.7965401530972968\\n- S4: 0.7328756796573327\\n- S5: 0.2907626799653432\\n\\nKurtosis:\\n- BMI: 0.08047812866814441\\n- BP: -0.5403332293804959\\n- S2: 0.5810556912433777\\n- S3: 0.9568955290670629\\n- S4: 0.42584638305987266\\n- S5: -0.14639566230288992\\n\\nNA Count:\\n- BMI: 0\\n- BP: 0\\n- S2: 0\\n- S3: 0\\n- S4: 0\\n- S5: 0\\n\\nQuantiles:\\n- BMI: [23.2, 25.7, 29.3]\\n- BP: [84.0, 93.0, 105.0]\\n- S2: [96.0, 113.0, 134.6]\\n- S3: [40.0, 48.0, 58.0]\\n- S4: [3.0, 4.0, 5.0]\\n- S5: [4.2767, 4.6151, 4.9972]\\n\\nTotal Instances: 884\\n\\n\\n--- diabetes_synthetic_data_summary.csv ---\\nUnique Values Count:\\n- AGE: 47\\n- SEX: 2\\n- BMI: 100\\n- BP: 100\\n- S1: 68\\n- S2: 100\\n- S3: 100\\n- S4: 100\\n- S5: 100\\n- S6: 37\\n- Y: 79\\n\\nMode:\\n- AGE: 47\\n- SEX: 2\\n- BMI: 19.007354352972975\\n- BP: 93.42271729856616\\n- S1: 235\\n- S2: 139.17466136596107\\n- S3: 40.70386202475124\\n- S4: 3.828560635385287\\n- S5: 4.211264147962045\\n- S6: 82\\n- Y: 30\\n\\nMode Count:\\n- AGE: 5\\n- SEX: 53\\n- BMI: 1\\n- BP: 1\\n- S1: 4\\n- S2: 1\\n- S3: 1\\n- S4: 1\\n- S5: 1\\n- S6: 6\\n- Y: 3\\n\\nNA Count:\\n- AGE: 0\\n- SEX: 0\\n- BMI: 0\\n- BP: 0\\n- S1: 0\\n- S2: 0\\n- S3: 0\\n- S4: 0\\n- S5: 0\\n- S6: 0\\n- Y: 0\\n\\nTotal Instances: 100'}, {'dataset_name': 'holidays', 'prompt': '--- holidays_summary.csv Statistics Report ---\\n\\nFor the \"BMI\" column:\\n- Mean: 26.38\\n- Standard Deviation: 4.41\\n- Minimum value: 18.0\\n- Maximum value: 42.2\\n- Skewness: 0.60\\n- Kurtosis: 0.08\\n- NA Count: 0\\n- Quantiles: [23.2, 25.7, 29.3]\\n- Total Instances: 13702\\n\\nFor the \"BP\" column:\\n- Mean: 94.65\\n- Standard Deviation: 13.82\\n- Minimum value: 62.0\\n- Maximum value: 133.0\\n- Skewness: 0.29\\n- Kurtosis: -0.54\\n- NA Count: 0\\n- Quantiles: [84.0, 93.0, 105.0]\\n- Total Instances: 13702\\n\\nFor the \"S2\" column:\\n- Mean: 115.44\\n- Standard Deviation: 30.38\\n- Minimum value: 41.6\\n- Maximum value: 242.4\\n- Skewness: 0.44\\n- Kurtosis: 0.58\\n- NA Count: 0\\n- Quantiles: [96.0, 113.0, 134.6]\\n- Total Instances: 13702\\n\\nFor the \"S3\" column:\\n- Mean: 49.79\\n- Standard Deviation: 12.92\\n- Minimum value: 22.0\\n- Maximum value: 99.0\\n- Skewness: 0.80\\n- Kurtosis: 0.96\\n- NA Count: 0\\n- Quantiles: [40.0, 48.0, 58.0]\\n- Total Instances: 13702\\n\\nFor the \"S4\" column:\\n- Mean: 4.07\\n- Standard Deviation: 1.29\\n- Minimum value: 2.0\\n- Maximum value: 9.09\\n- Skewness: 0.73\\n- Kurtosis: 0.43\\n- NA Count: 0\\n- Quantiles: [3.0, 4.0, 5.0]\\n- Total Instances: 13702\\n\\nFor the \"S5\" column:\\n- Mean: 4.64\\n- Standard Deviation: 0.52\\n- Minimum value: 3.26\\n- Maximum value: 6.11\\n- Skewness: 0.29\\n- Kurtosis: -0.15\\n- NA Count: 0\\n- Quantiles: [4.28, 4.62, 5.00]\\n- Total Instances: 13702\\n\\n\\n--- holidays_synthetic_data_summary.csv Statistics Report ---\\n\\nFor the \"AGE\" column:\\n- Unique Values Count: 42\\n- Mode: 52\\n- Mode Count: 5\\n- NA Count: 0\\n- Total Instances: 100\\n\\nFor the \"SEX\" column:\\n- Unique Values Count: 2\\n- Mode: 2\\n- Mode Count: 57\\n- NA Count: 0\\n- Total Instances: 100\\n\\nFor the \"BMI\" column:\\n- Unique Values Count: 100\\n- Mode: 31.15\\n- Mode Count: 1\\n- NA Count: 0\\n- Total Instances: 100\\n\\nFor the \"BP\" column:\\n- Unique Values Count: 100\\n- Mode: 85.27\\n- Mode Count: 1\\n- NA Count: 0\\n- Total Instances: 100\\n\\nFor the \"S1\" column:\\n- Unique Values Count: 74\\n- Mode: 175\\n- Mode Count: 4\\n- NA Count: 0\\n- Total Instances: 100\\n\\nFor the \"S2\" column:\\n- Unique Values Count: 100\\n- Mode: 98.41\\n- Mode Count: 1\\n- NA Count: 0\\n- Total Instances: 100\\n\\nFor the \"S3\" column:\\n- Unique Values Count: 100\\n- Mode: 67.42\\n- Mode Count: 1\\n- NA Count: 0\\n- Total Instances: 100\\n\\nFor the \"S4\" column:\\n- Unique Values Count: 100\\n- Mode: 2.97\\n- Mode Count: 1\\n- NA Count: 0\\n- Total Instances: 100\\n\\nFor the \"S5\" column:\\n- Unique Values Count: 100\\n- Mode: 4.47\\n- Mode Count: 1\\n- NA Count: 0\\n- Total Instances: 100\\n\\nFor the \"S6\" column:\\n- Unique Values Count: 37\\n- Mode: 96\\n- Mode Count: 8\\n- NA Count: 0\\n- Total Instances: 100\\n\\nFor the \"Y\" column:\\n- Unique Values Count: 80\\n- Mode: 120\\n- Mode Count: 5\\n- NA Count: 0\\n- Total Instances: 100'}]"

# CELL ********************

data_dicts_list = ast.literal_eval(gpt_response)

# CELL ********************

!pip install Spire.Pdf

# CELL ********************

from spire.pdf.common import *
from spire.pdf import *

# CELL ********************

filepath_base = '/lakehouse/default/Files'

# CELL ********************

def generate_reports(data_dict, filepath_base):
    dataset_name = data_dict['dataset_name']
    prompt = data_dict['prompt']

    # generate names and folders
    report_path_pdf = os.path.join(filepath_base, 'report' + dataset_name + '_' + datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + '.pdf')
    report_subfolder = os.path.join(filepath_base, 'report' + dataset_name + '_' + datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
    os.mkdir(report_subfolder)

    # create the reporting structure for pdf
    my_doc = SimpleDocTemplate(report_path_pdf)
    flowables = []
    sample_style_sheet = getSampleStyleSheet()
    paragraph_1 = Paragraph("Data report for " + dataset_name, sample_style_sheet['Heading1'])

    # add descriptives from gpt
    gpt_text = str(prompt).replace('\n','<br />\n')

    paragraph_2 = Paragraph(
        gpt_text,
        sample_style_sheet['BodyText']
    )
    flowables.append(paragraph_1)
    flowables.append(paragraph_2)

    # add data from training folder
    train_folder = os.path.join(filepath_base, 'Similarity_report_' + dataset_name)
    train_files = os.listdir(train_folder)
    train_images = [filename for filename in train_files if filename[-3:] == 'png']
    train_text = [filename for filename in train_files if filename[-3:] == 'txt']
    model_output = os.path.join(filepath_base, dataset_name + '_synthetic_model_report.txt')

    # add images to pdf
    if len(train_images) > 0:
        # this is the 'normal' pagesize
        img0 = 439
        img1 = 685
        for img_name in train_images:
            img_path = os.path.join(train_folder, img_name)
            image = mimg.imread(img_path)
            print(img_name)
            print(image.shape)
            x_diff = image.shape[1] - img0
            y_diff = image.shape[0] - img1
            print(f'xdiff is {x_diff} and ydiff is {y_diff}')
            if ((x_diff > 0) and (x_diff >= y_diff)):
                ratio = img0/image.shape[1]
                im_shape0 = img0
                im_shape1 = image.shape[0]*ratio
                print(f'resized is {im_shape0} {im_shape1}')
                flowables.append(rpl.Image(img_path, width=im_shape0, height=im_shape1))
            elif ((y_diff > 0) and (y_diff >= x_diff)):
                ratio = img1/image.shape[0]
                print(ratio)
                im_shape1 = img1
                im_shape0 = image.shape[1]*ratio
                print(f'resized is {im_shape0} {im_shape1}')
                flowables.append(rpl.Image(img_path, width=im_shape0, height=im_shape1))
            else:
                print(f'not resized is {image.shape}')
                flowables.append(rpl.Image(img_path))

    if len(train_text) > 0:
        flowables.append(PageBreak())
        for text_file in train_text:
            text_path = os.path.join(train_folder, text_file)
            with open(os.path.join(text_path)) as f:
                contents = f.read()
                contents = str(contents).replace('\n','<br />\n')
            paragraph_3 = Paragraph(contents, sample_style_sheet['BodyText'])
            flowables.append(paragraph_3)

    try:
        flowables.append(PageBreak())
        with open(model_output) as f:
            contents = f.read()
            contents = str(contents).replace('\n','<br />\n')
        paragraph_4 = Paragraph(contents, sample_style_sheet['BodyText'])
        flowables.append(paragraph_4)
    except:
        print('failed')


    my_doc.build(flowables)

    # Create a PdfDocument object
    doc = PdfDocument()
    # Load a PDF document
    doc.LoadFromFile(report_path_pdf)

    # Loop through the pages in the document
    for i in range(doc.Pages.Count):
        # Save each page as a PNG image
        fileName = os.path.join(report_subfolder, 'page_'+ str(i) +'.png')
        with doc.SaveAsImage(i) as imageS:
            imageS.Save(fileName)

    # Close the PdfDocument object
    doc.Close()


# CELL ********************

for data_dict in data_dicts_list:
    generate_reports(data_dict, filepath_base)
