import pandas as pd
import requests
import os

if __name__ == '__main__':

    if not os.path.exists('../Data'):
        os.mkdir('../Data')

    # Download data if it is unavailable.
    if ('A_office_data.xml' not in os.listdir('../Data') and
        'B_office_data.xml' not in os.listdir('../Data') and
        'hr_data.xml' not in os.listdir('../Data')):
        print('A_office_data loading.')
        url = "https://www.dropbox.com/s/jpeknyzx57c4jb2/A_office_data.xml?dl=1"
        r = requests.get(url, allow_redirects=True)
        open('../Data/A_office_data.xml', 'wb').write(r.content)
        print('Loaded.')

        print('B_office_data loading.')
        url = "https://www.dropbox.com/s/hea0tbhir64u9t5/B_office_data.xml?dl=1"
        r = requests.get(url, allow_redirects=True)
        open('../Data/B_office_data.xml', 'wb').write(r.content)
        print('Loaded.')

        print('hr_data loading.')
        url = "https://www.dropbox.com/s/u6jzqqg1byajy0s/hr_data.xml?dl=1"
        r = requests.get(url, allow_redirects=True)
        open('../Data/hr_data.xml', 'wb').write(r.content)
        print('Loaded.')

        # All data in now loaded to the Data folder.

def load_datasets():
    a_df = pd.read_xml('../Data/A_office_data.xml')
    b_df = pd.read_xml('../Data/B_office_data.xml')
    hr_df = pd.read_xml('../Data/hr_data.xml')
    return a_df, b_df, hr_df

def set_indexes(a_df, b_df, hr_df):
    # set the indexes of all dataframes as the employee ID#
    hr_df.set_index('employee_id', inplace=True)
    char = 'A'
    for office in [a_df, b_df]:
        emp_id = []
        for i in office['employee_office_id']:
            emp_id.append(f'{char}{i}')
        if char == 'A':
            a_df.index = emp_id
            char = 'B'
        elif char == 'B':
            b_df.index = emp_id
    return a_df, b_df, hr_df

def unify_datasets(a_df, b_df, hr_df):
    # combine office A with office B
    ab_df = pd.concat([a_df, b_df])

    # merge combined offices with hr data to employee id (index)
    new_df = hr_df.merge(ab_df, left_index=True, right_index=True)
    new_df.drop(columns=['employee_office_id'], inplace=True)
    new_df.sort_index(inplace=True)
    return new_df


def count_bigger_5(number_project):
    count = 0
    for project in number_project:
        if project > 5:
            count += 1
    return count


a_df, b_df, hr_df = load_datasets()
a_df, b_df, hr_df = set_indexes(a_df, b_df, hr_df)
unified_df = unify_datasets(a_df, b_df, hr_df)

pd.set_option("display.max_columns", 10)

hr_request = round(unified_df.groupby(['left']).agg({'number_project': ['median', count_bigger_5],
                                                     'time_spend_company': ['mean', 'median'],
                                                     'Work_accident': 'mean',
                                                     'last_evaluation': ['mean', 'std']}), 2)

print(hr_request.to_dict())