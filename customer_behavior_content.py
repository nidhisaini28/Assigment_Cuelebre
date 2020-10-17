import pandas as pd

file_path = input("enter file path: ")

# read  data from files
file_start_stream = pd.read_csv(file_path + "\\started_streams.csv", sep=';')
file_whatson = pd.read_csv(file_path + "\\whatson.csv")


def sales_rental_broadcast_rights(file_start_stream, file_whatson):
    # mapping country with country code
    countries = {'country_code': ['no', 'dk', 'se', 'fi'],
                 'country':  ['Norway', 'Denmark', 'Sweden', 'Finland']
                 }

    df_country = pd.DataFrame(countries, columns=['country_code', 'country'])

    # merge watson country code into watson data
    df_watson = pd.merge(file_whatson, df_country, left_on='broadcast_right_region', right_on='country', how='inner')

    # joining two data tables(dataframe) for product type = 'tvot' and 'est' for
    # most recent date based on house_number and country
    df_merge = pd.merge(file_start_stream, df_watson, on=['house_number', 'country_code'], how='inner')

    merge_data = df_merge.loc[(df_merge.product_type == 'tvod') | (df_merge.product_type == 'est')]

    output_data = merge_data.sort_values(by=['dt_y']).groupby(['house_number', 'country_code']).last().reset_index()
    output_data2 = output_data[
        ['dt_y', 'time', 'device_name', 'house_number', 'user_id', 'country_code', 'program_title', 'season',
         'season_episode', 'genre', 'product_type', 'broadcast_right_start_date', 'broadcast_right_end_date']]
    task1_output = output_data2.rename(columns={'dt_y': 'dt'}, inplace=False)
    print(task1_output)
    return task1_output


def product_user_count(file_start_stream):
    # finding the number of unique user and number of watches a product is getting based on date,
    # program title,device_name,country_code
    product_watches = file_start_stream.groupby(
        ['product_type', 'dt', 'program_title', 'device_name', 'country_code']).count().reset_index()
    unique_user_count = \
        file_start_stream.groupby(['product_type', 'dt', 'program_title', 'device_name', 'country_code'])[
            'user_id'].nunique().reset_index()

    # merging two dataframes
    product_watches_a = product_watches[
        ['dt', 'program_title', 'device_name', 'country_code', 'product_type', 'user_id']]
    product_watches_b = product_watches_a.rename(columns={'user_id': 'content_count'}, inplace=False)
    task2_output = pd.merge(product_watches_b, unique_user_count,
                            on=['dt', 'program_title', 'device_name', 'country_code', 'product_type'])

    task2_output.sort_values('device_name')
    print(task2_output)
    return task2_output


def genre_time_day(file_start_stream):
    # creating a column containing the hour(time) that people watch
    file_start_stream['watched_hour'] = pd.to_datetime(file_start_stream['time'], format='%H:%M:%S').dt.hour

    # finding the most popular genre based  hour(time) people watch
    task3_output = file_start_stream.groupby(['watched_hour', 'genre'])['user_id'].nunique().reset_index().sort_values(
        'user_id', ascending=False)
    print(task3_output)
    return task3_output


final_output_task1 = sales_rental_broadcast_rights(file_start_stream, file_whatson)
final_output_task2 = product_user_count(file_start_stream)
final_output_task3 = genre_time_day(file_start_stream)